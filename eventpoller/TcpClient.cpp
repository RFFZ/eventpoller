#include "TcpClient.h"
#include <iostream>
#include <cstring>

// ════════════════════════════════════════════════════════════════
//  公共部分：构造/析构/connect入口/send入口/close，两平台逻辑一致
// ════════════════════════════════════════════════════════════════

TcpClient::TcpClient(EventPoller::Ptr poller)
    : _poller(std::move(poller)) {
    _recv_buf.resize(RECV_BUF_SIZE);
}

TcpClient::~TcpClient() {
    close();
}

void TcpClient::connect(const std::string& host, uint16_t port, int timeout_ms) {
    sockaddr_in addr{};
    if (!resolveAddress(host, port, addr)) {
        // 解析失败，切到poller线程统一触发回调（保持回调始终在poller线程的约定）
        _poller->async([this, self = shared_from_this()]{
            handleConnected(false, "resolve address failed");
            });
        return;
    }
    // doConnect 内部会创建socket、发起非阻塞connect，必须在poller线程里做
    // （fd的所有操作都只允许在poller线程，这是整个架构的线程安全前提）
    _poller->async([this, self = shared_from_this(), addr, timeout_ms]{
        doConnect(addr, timeout_ms);
        });
}

void TcpClient::send(const char* data, int len) {
    if (len <= 0) return;
    std::vector<char> buf(data, data + len);
    _poller->async([this, self = shared_from_this(), buf = std::move(buf)]() mutable {
        if (!_connected) return;   // 连接已断开，丢弃
#ifndef _WIN32
        _send_queue.push_back(std::move(buf));
        tryFlushSendQueue();
#else
        doSend(std::move(buf));
#endif
    });
}

void TcpClient::send(const std::string& data) {
    send(data.data(), (int)data.size());
}

void TcpClient::close() {
    if (_fd == INVALID_SOCK) return;
    _poller->async([this, self = shared_from_this()]{
        cleanup();
        });
}

void TcpClient::cleanup() {
    if (_fd == INVALID_SOCK) return;
    //std::cout<<"TcpClient close cleanup"<<std::endl;
    if (_connect_timeout_timer) {
        _poller->cancelTimer(_connect_timeout_timer);
        _connect_timeout_timer = 0;
    }
#ifndef _WIN32
    _poller->delEvent(_fd);
#endif
    closeSocket(_fd);
    _fd = INVALID_SOCK;
    _connected = false;
    _connecting = false;
}

void TcpClient::handleConnected(bool ok, const std::string& err) {
    if (_connect_timeout_timer) {
        _poller->cancelTimer(_connect_timeout_timer);
        _connect_timeout_timer = 0;
    }
    _connecting = false;
    if (ok) {
        _connected = true;
    }
    onConnect(ok, err);
    if (!ok) {
        cleanup();
    }
}


// ════════════════════════════════════════════════════════════════
//  Linux/epoll 路径
// ════════════════════════════════════════════════════════════════
#ifndef _WIN32

void TcpClient::doConnect(const sockaddr_in& addr, int timeout_ms) {
    _fd = createTcpSocket();
    if (_fd == INVALID_SOCK) {
        handleConnected(false, "create socket failed");
        return;
    }
    _connecting = true;

    int ret = ::connect(_fd, (const sockaddr*)&addr, sizeof(addr));
    if (ret == 0) {
        // 极少数情况：本机回环地址，立刻连接成功
        _poller->modEvent(_fd, EV_READ,
            [this, self = shared_from_this()](int ev){ onReadable(ev); });
        handleConnected(true, "");
        return;
    }
    if (SOCK_ERRNO != ERR_INPROGRESS) {
        handleConnected(false, strerror(errno));
        return;
    }

    // 正常情况：连接进行中，监听可写事件判断结果
    _poller->addEvent(_fd, EV_WRITE,
        [this, self = shared_from_this()](int ev){ onWritableConnecting(ev); });

    // 连接超时定时器
    _connect_timeout_timer = _poller->doDelay((uint64_t)timeout_ms,
        [this, self = shared_from_this()]{
            if (_connecting) {
                cleanup();
                onConnect(false, "connect timeout");
            }
        });
}

void TcpClient::onWritableConnecting(int events) {
    // EPOLLOUT 触发了，说明connect有结果了（成功或失败），用getsockopt查真实结果
    int err = 0;
    socklen_t len = sizeof(err);
    getsockopt(_fd, SOL_SOCKET, SO_ERROR, &err, &len);

    if (err != 0) {
        cleanup();
        onConnect(false, strerror(err));
        return;
    }

    // 连接成功，切换成监听可读事件
    _poller->modEvent(_fd, EV_READ,
        [this, self = shared_from_this()](int ev){ onReadable(ev); });
    handleConnected(true, "");
}

void TcpClient::onReadable(int events) {
    if (events & EV_READ) {
        while (true) {
            int n = (int)::read(_fd, _recv_buf.data(), _recv_buf.size());
            if (n > 0) {
                onRecv(_recv_buf.data(), n);
                if (n < (int)_recv_buf.size()) break;  // 没读满，说明这次数据读完了
                continue;                                // 读满了，可能还有数据，继续读
            }
            if (n == 0) {
                // 对端正常关闭连接
                cleanup();
                onError("epoll read 0 peer closed connection");
                return;
            }
            // n < 0
            if (errno == ERR_WOULDBLOCK) break;   // 数据读完了
            if (errno == EINTR) continue;
            cleanup();
            onError(strerror(errno));
            return;
        }
    }
    if (events & EV_WRITE) {
        // 读事件回调里偶尔也可能带着可写标志（理论上不会，因为我们用modEvent切换了监听类型）
        tryFlushSendQueue();
    }
}

void TcpClient::onWritableSending(int events) {
    tryFlushSendQueue();
}

void TcpClient::tryFlushSendQueue() {
    while (!_send_queue.empty()) {
        auto& front = _send_queue.front();
        const char* p = front.data() + _send_offset;
        size_t      len = front.size() - _send_offset;

        int n = (int)::write(_fd, p, len);
        if (n > 0) {
            _send_offset += n;
            if (_send_offset >= front.size()) {
                _send_queue.pop_front();
                _send_offset = 0;
            }
            continue;   // 继续尝试发下一块/剩余部分
        }
        if (n < 0 && (errno == ERR_WOULDBLOCK)) {
            // 内核发送缓冲区满了，注册EPOLLOUT等下次可写再继续
            _poller->modEvent(_fd, EV_READ | EV_WRITE,
                [this, self = shared_from_this()](int ev){
                if (ev & EV_READ)  onReadable(ev);
                if (ev & EV_WRITE) onWritableSending(ev);
            });
            return;
        }
        if (n < 0 && errno == EINTR) continue;

        // 其他错误：连接出问题了
        cleanup();
        onError(strerror(errno));
        return;
    }

    // 发送队列空了，切回只监听可读（不需要EPOLLOUT了）
    _poller->modEvent(_fd, EV_READ,
        [this, self = shared_from_this()](int ev){ onReadable(ev); });
    onSendComplete();
}

#endif // !_WIN32


// ════════════════════════════════════════════════════════════════
//  Windows/IOCP 路径
// ════════════════════════════════════════════════════════════════
#ifdef _WIN32

void TcpClient::doConnect(const sockaddr_in& addr, int timeout_ms) {
    _fd = createTcpSocket();
    if (_fd == INVALID_SOCK) {
        handleConnected(false, "create socket failed");
        return;
    }

    // ConnectEx 要求：socket 必须先 bind 一个本地地址（端口填0让系统分配）
    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = INADDR_ANY;
    local.sin_port = 0;
    if (::bind(_fd, (sockaddr*)&local, sizeof(local)) == SOCKET_ERROR) {
        handleConnected(false, "bind failed");
        return;
    }

    // 关联到 IOCP（必须在任何 postXxx 之前完成）
    if (!_poller->associateSocket(_fd)) {
        handleConnected(false, "associateSocket failed");
        return;
    }

    _connecting = true;

    // 投递异步连接，完成（成功/失败）时 IOCP 完成包会被 runLoop 的 Connect 分支接住
    bool ok = _poller->postConnect(_fd, addr,
        [this, self = shared_from_this()](bool success, int err_code) {
        // 这个回调已经在 IOCP 线程里执行，不需要切线程
        if (success) {
            handleConnected(true, "");
            postNextRecv();   // 连接成功，立刻开始持续读取
        }
        else {
            cleanup();
            onConnect(false, "connect failed, err=" + std::to_string(err_code));
        }
    });

    if (!ok) {
        _connecting = false;
        cleanup();
        onConnect(false, "postConnect failed");
        return;
    }

    // 连接超时定时器：如果 IOCP 完成包在 timeout_ms 内没来，主动放弃
    _connect_timeout_timer = _poller->doDelay((uint64_t)timeout_ms,
        [this, self = shared_from_this()]{
            if (_connecting) {
                cleanup();
                onConnect(false, "connect timeout");
            }
        });
}

// ── postNextRecv：投递一次异步读，完成后在回调里再投递下一次 ──────
// 这样形成"连续读"的循环，效果等价于 epoll 里"只要可读就一直read"
void TcpClient::postNextRecv() {
    if (!_connected && !_connecting) return;
    if (_fd == INVALID_SOCK) return;
    //std::cout << "TcpClient::postNextRecv" << std::endl;
    bool ok = _poller->postRecv(_fd, _recv_buf.data(), (int)_recv_buf.size(),
        [this, self = shared_from_this()](bool success, const char* buf, int size) {
        if (!success || size == 0) {
            // size==0 表示对端正常关闭；!success 表示出错
            cleanup();
            onError(size == 0 ? "iocp postRecv peer closed connection" : "recv failed");
            return;
        }
        onRecv(buf, size);
        // 处理完这次数据，立刻投递下一次读，维持持续读取
        postNextRecv();
    });

    if (!ok) {
        cleanup();
        onError("postRecv failed");
    }
}

// ── doSend：投递一次异步写 ─────────────────────────────────────────
void TcpClient::doSend(std::vector<char> data) {
    if (!_connected || _fd == INVALID_SOCK) return;

    _pending_sends++;
    // data 的生命周期必须保持到 WSASend 真正完成，所以用 shared_ptr 包一层，
    // 让回调持有它，IOCP完成之前数据不会被释放
    auto buf_holder = std::make_shared<std::vector<char>>(std::move(data));

    bool ok = _poller->postSend(_fd, buf_holder->data(), (int)buf_holder->size(),
        [this, self = shared_from_this(), buf_holder](bool success, const char*, int size) {
        _pending_sends--;
        if (!success) {
            cleanup();
            onError("send failed");
            return;
        }
        if (_pending_sends == 0) {
            onSendComplete();
        }
    });

    if (!ok) {
        _pending_sends--;
        cleanup();
        onError("postSend failed");
    }
}

#endif // _WIN32