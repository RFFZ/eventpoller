// EventPoller.cpp
//
// 同一个文件里同时包含 Windows IOCP 实现和 Linux epoll 实现，
// 用 #ifdef _WIN32 / #else 整段切换，编译期只会留下对应平台的代码。

#include "EventPoller.h"
#include <cassert>
#include <iostream>
#include <cstring>
#include <stdexcept>
#include <algorithm>

// ════════════════════════════════════════════════════════════════
//  公共部分：任务队列相关逻辑两个平台完全一样
//  （写在最上面，下面平台分支里复用）
// ════════════════════════════════════════════════════════════════

void EventPoller::flushTasks() {
    std::queue<Task> local;
    {
        std::lock_guard<std::mutex> lk(_task_mtx);
        std::swap(local, _tasks);
    }
    while (!local.empty()) {
        local.front()();
        local.pop();
    }
}

void EventPoller::async(Task task) {
    {
        std::lock_guard<std::mutex> lk(_task_mtx);
        _tasks.push(std::move(task));
    }
    if (!isCurrentThread()) wakeup();
}

void EventPoller::sync(Task task) {
    assert(!isCurrentThread() && "sync() in poller thread -> deadlock!");
    std::mutex mtx;
    std::condition_variable cv;
    bool done = false;
    async([&] {
        task();
        std::unique_lock<std::mutex> lk(mtx);
        done = true;
        cv.notify_one();
        });
    std::unique_lock<std::mutex> lk(mtx);
    cv.wait(lk, [&] { return done; });
}

bool EventPoller::isCurrentThread() const {
    return std::this_thread::get_id() == _thread_id;
}

void EventPoller::start() {
    if (_running.exchange(true)) return;
    _thread = std::thread([this] { runLoop(); });
}


// ════════════════════════════════════════════════════════════════
//  公共部分：定时器逻辑，两个平台完全一样
// ════════════════════════════════════════════════════════════════

uint64_t EventPoller::nowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        steady_clock::now().time_since_epoch()
        ).count();
}

// doDelay/doRepeat/cancelTimer 都可能从任意线程调用，
// 但 _timer_heap 只能在 poller 线程里访问，所以统一通过 async() 切换线程。
TimerId EventPoller::doDelay(uint64_t delay_ms, Task task) {
    TimerId id = _timer_id_gen.fetch_add(1);
    auto flag = std::make_shared<bool>(false);

    TimerTask t;
    t.expire_ms = nowMs() + delay_ms;
    t.interval_ms = 0;            // 一次性
    t.task = std::move(task);
    t.id = id;
    t.canceled = flag;

    async([this, t = std::move(t), flag]() mutable {
        _timer_flags[t.id] = flag;
        _timer_heap.push(std::move(t));
    });
    return id;
}

TimerId EventPoller::doRepeat(uint64_t interval_ms, Task task) {
    TimerId id = _timer_id_gen.fetch_add(1);
    auto flag = std::make_shared<bool>(false);

    TimerTask t;
    t.expire_ms = nowMs() + interval_ms;
    t.interval_ms = interval_ms;  // >0，到期后会自动重新排期
    t.task = std::move(task);
    t.id = id;
    t.canceled = flag;

    async([this, t = std::move(t), flag]() mutable {
        _timer_flags[t.id] = flag;
        _timer_heap.push(std::move(t));
    });
    return id;
}

void EventPoller::cancelTimer(TimerId id) {
    // 直接在调用线程把 flag 置 true 即可，不需要切到 poller 线程，
    // 因为 shared_ptr<bool> 的读写在这里只有 bool 赋值，没有线程安全问题
    // （poller 线程只读这个 flag，不写）
    async([this, id] {
        auto it = _timer_flags.find(id);
        if (it != _timer_flags.end()) {
            *(it->second) = true;
            _timer_flags.erase(it);
        }
        });
}

// 处理所有到期定时器，并返回距离下一个定时器还有多久(ms)
// 返回 -1 表示没有定时器，可以无限等待
int64_t EventPoller::processTimersAndGetNextDelay() {
    uint64_t now = nowMs();

    while (!_timer_heap.empty()) {
        const TimerTask& top = _timer_heap.top();

        // 已取消的定时器，直接丢弃，不执行也不重新排期
        if (*top.canceled) {
            _timer_heap.pop();
            continue;
        }

        if (top.expire_ms > now) {
            // 堆顶还没到期，返回还要等多久
            return (int64_t)(top.expire_ms - now);
        }

        // 到期了：拷贝出来执行，因为执行过程中可能会修改堆（比如定时器内部又加定时器）
        TimerTask fired = top;
        _timer_heap.pop();

        // 执行任务本体
        if (fired.task) fired.task();

        // 周期性定时器：重新计算下次到期时间，塞回堆里
        if (fired.interval_ms > 0 && !*fired.canceled) {
            fired.expire_ms = nowMs() + fired.interval_ms;
            _timer_heap.push(fired);
        }
        else {
            // 一次性定时器执行完，清理 flag 表
            _timer_flags.erase(fired.id);
        }
    }
    return -1;  // 堆空了，没有定时器
}


// ════════════════════════════════════════════════════════════════
//  Windows 实现：IOCP
// ════════════════════════════════════════════════════════════════
#ifdef _WIN32

// ── 构造/析构 ────────────────────────────────────────────────────
EventPoller::EventPoller() {
    // 创建 IOCP 内核对象。
    // 第2个参数传 nullptr 表示新建一个完成端口；
    // 第4个参数(并发线程数) 传 1，因为我们只用一个线程跑 GetQueuedCompletionStatus
    _iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 1);
    if (!_iocp) throw std::runtime_error("CreateIoCompletionPort failed");

    // 准备一个专用于"唤醒"的 IoContext。
    // IOCP 没有 pipe 概念，唤醒靠 PostQueuedCompletionStatus 投递一个假完成包。
    _wake_ctx = new IoContext();
    ZeroMemory(static_cast<OVERLAPPED*>(_wake_ctx), sizeof(OVERLAPPED));
    _wake_ctx->type = OpType::Wake;
}

EventPoller::~EventPoller() {
    stop();
    if (_iocp) { CloseHandle(_iocp); _iocp = nullptr; }
    delete _wake_ctx;
}

// ── stop ─────────────────────────────────────────────────────────
void EventPoller::stop() {
    if (!_running.exchange(false)) return;
    wakeup();
    if (_thread.joinable()) _thread.join();
}

// ── wakeup：投递一个假的完成包，把 GetQueuedCompletionStatus 唤醒 ──
void EventPoller::wakeup() {
    // 第2、3参数(字节数/key)对 Wake 类型没有意义，随便填0
    PostQueuedCompletionStatus(_iocp, 0, 0, _wake_ctx);
}

// ── associateSocket：把 socket 绑定到 IOCP ───────────────────────
bool EventPoller::associateSocket(sock_t fd) {
    // key 传 0：我们不靠 IOCP 的 completion key 区分 socket，
    // 而是从 IoContext::fd 字段里取，更灵活。
    HANDLE h = CreateIoCompletionPort((HANDLE)fd, _iocp, 0, 0);
    return h != nullptr;
}

// ── postRecv：投递一次异步读 ──────────────────────────────────────
bool EventPoller::postRecv(sock_t fd, char* buf, int len, IoCallback cb) {
    auto* ctx = new IoContext();
    ZeroMemory(static_cast<OVERLAPPED*>(ctx), sizeof(OVERLAPPED));
    ctx->type = OpType::Recv;
    ctx->fd = fd;
    ctx->wsabuf.buf = buf;
    ctx->wsabuf.len = (ULONG)len;
    ctx->cb = std::move(cb);

    DWORD flags = 0, bytes = 0;
    int ret = WSARecv(fd, &ctx->wsabuf, 1, &bytes, &flags,
        static_cast<OVERLAPPED*>(ctx), nullptr);

    if (ret == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
        // 立即失败（非"正在处理中"）
        delete ctx;
        return false;
    }
    // ret==0 表示同步立即完成，也会走 IOCP 通知，不需要在这里额外处理
    return true;
}

// ── postSend：投递一次异步写 ──────────────────────────────────────
bool EventPoller::postSend(sock_t fd, const char* buf, int len, IoCallback cb) {
    auto* ctx = new IoContext();
    ZeroMemory(static_cast<OVERLAPPED*>(ctx), sizeof(OVERLAPPED));
    ctx->type = OpType::Send;
    ctx->fd = fd;
    // WSASend 的 buf 是 char*，这里 const_cast 一下（不会被修改）
    ctx->wsabuf.buf = const_cast<char*>(buf);
    ctx->wsabuf.len = (ULONG)len;
    ctx->cb = std::move(cb);

    DWORD bytes = 0;
    int ret = WSASend(fd, &ctx->wsabuf, 1, &bytes, 0,
        static_cast<OVERLAPPED*>(ctx), nullptr);

    if (ret == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
        delete ctx;
        return false;
    }
    return true;
}

// ── runLoop：IOCP 主循环 ──────────────────────────────────────────
void EventPoller::runLoop() {
    _thread_id = std::this_thread::get_id();

    while (_running) {
        // 先处理到期定时器，顺便算出下次还要等多久
        int64_t delay = processTimersAndGetNextDelay();
        // GetQueuedCompletionStatus 的超时是 DWORD(ms)，-1表示无限等待用INFINITE
        DWORD wait_ms = (delay < 0) ? 1000   // 没有定时器时也最多等1秒，保证能及时退出
            : (DWORD)std::min<int64_t>(delay, 1000);

        DWORD       bytes = 0;
        ULONG_PTR   key = 0;
        OVERLAPPED* overlapped = nullptr;

        BOOL ok = GetQueuedCompletionStatus(_iocp, &bytes, &key,
            &overlapped, wait_ms);

        if (!ok && overlapped == nullptr) {
            // 超时（没有任何完成包），回到循环开头重新计算定时器
            continue;
        }

        auto* ctx = static_cast<IoContext*>(overlapped);
        if (!ctx) continue;

        if (ctx->type == OpType::Wake) {
            // 唤醒包，不携带真实数据，只用来跳出 GetQueuedCompletionStatus
            flushTasks();
            continue;
        }

        // ── 处理真实 IO 完成 ─────────────────────────────────────
        bool success = ok && bytes > 0;

        if (ctx->type == OpType::Recv) {
            if (ctx->cb) ctx->cb(success, ctx->wsabuf.buf, (int)bytes);
        }
        else if (ctx->type == OpType::Send) {
            if (ctx->cb) ctx->cb(success, nullptr, (int)bytes);
        }

        delete ctx;
        flushTasks();
    }

    std::cout << "[EventPoller/IOCP] loop exited\n";
}


// ════════════════════════════════════════════════════════════════
//  Linux/macOS 实现：epoll
// ════════════════════════════════════════════════════════════════
#else  // !_WIN32

#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>

// EV_READ/EV_WRITE → epoll events
static uint32_t toEpollEvents(int ev) {
    uint32_t e = 0;
    if (ev & EV_READ)  e |= EPOLLIN;
    if (ev & EV_WRITE) e |= EPOLLOUT;
    return e;
}
static int fromEpollEvents(uint32_t e) {
    int ev = 0;
    if (e & (EPOLLIN | EPOLLHUP | EPOLLERR)) ev |= EV_READ;
    if (e & (EPOLLOUT | EPOLLERR))             ev |= EV_WRITE;
    return ev;
}

// ── 构造/析构 ────────────────────────────────────────────────────
EventPoller::EventPoller() {
    _epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (_epoll_fd == -1) throw std::runtime_error("epoll_create1 failed");

    if (pipe2(_wake, O_CLOEXEC | O_NONBLOCK) == -1)
        throw std::runtime_error("pipe2 failed");

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = _wake[0];
    if (epoll_ctl(_epoll_fd, EPOLL_CTL_ADD, _wake[0], &ev) == -1)
        throw std::runtime_error("epoll_ctl add pipe failed");
}

EventPoller::~EventPoller() {
    stop();
    if (_epoll_fd != -1) close(_epoll_fd);
    if (_wake[0] != -1) close(_wake[0]);
    if (_wake[1] != -1) close(_wake[1]);
}

// ── stop ─────────────────────────────────────────────────────────
void EventPoller::stop() {
    if (!_running.exchange(false)) return;
    wakeup();
    if (_thread.joinable()) _thread.join();
}

// ── wakeup ───────────────────────────────────────────────────────
void EventPoller::wakeup() {
    char c = 1;
    (void)write(_wake[1], &c, 1);
}

// ── addEvent / modEvent / delEvent ───────────────────────────────
int EventPoller::addEvent(sock_t fd, int events, EventCallback cb) {
    epoll_event ev{};
    ev.events = toEpollEvents(events);
    ev.data.fd = fd;
    if (epoll_ctl(_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        std::cerr << "[epoll] addEvent fd=" << fd << " err=" << strerror(errno) << "\n";
        return -1;
    }
    _fds[fd] = { events, std::move(cb) };
    return 0;
}

int EventPoller::modEvent(sock_t fd, int events, EventCallback cb) {
    epoll_event ev{};
    ev.events = toEpollEvents(events);
    ev.data.fd = fd;
    if (epoll_ctl(_epoll_fd, EPOLL_CTL_MOD, fd, &ev) == -1) {
        std::cerr << "[epoll] modEvent fd=" << fd << " err=" << strerror(errno) << "\n";
        return -1;
    }
    _fds[fd] = { events, std::move(cb) };
    return 0;
}

int EventPoller::delEvent(sock_t fd) {
    if (epoll_ctl(_epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
        std::cerr << "[epoll] delEvent fd=" << fd << " err=" << strerror(errno) << "\n";
        return -1;
    }
    _fds.erase(fd);
    return 0;
}

// ── runLoop：epoll 主循环 ────────────────────────────────────────
void EventPoller::runLoop() {
    _thread_id = std::this_thread::get_id();
    epoll_event events[64];

    while (_running) {
        // 先处理到期定时器，顺便算出下次还要等多久
        int64_t delay = processTimersAndGetNextDelay();
        int timeout_ms = (delay < 0) ? -1 : (int)std::min<int64_t>(delay, INT32_MAX);

        int n = epoll_wait(_epoll_fd, events, 64, timeout_ms);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[epoll] epoll_wait err=" << strerror(errno) << "\n";
            break;
        }

        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == _wake[0]) {
                char buf[64];
                while (read(_wake[0], buf, sizeof(buf)) > 0) {}
            }
        }
        flushTasks();

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            if (fd == _wake[0]) continue;
            auto it = _fds.find(fd);
            if (it != _fds.end()) {
                int ev = fromEpollEvents(events[i].events);
                if (ev) it->second.cb(ev);
            }
        }
        // n==0 表示超时返回（定时器到期），下一轮循环开头会重新 processTimers
    }
    std::cout << "[EventPoller/epoll] loop exited\n";
}

#endif // _WIN32