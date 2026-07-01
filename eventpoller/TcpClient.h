#pragma once

#include "PlatformSocket.h"
#include "EventPoller.h"

#include <string>
#include <deque>
#include <vector>
#include <functional>
#include <mutex>
#include <atomic>
#include <memory>

// ================================================================
//  TcpClient
//
//  统一封装一条 TCP 连接的完整生命周期：异步connect、收发、关闭。
//  上层（比如 RtspPusher）只需要重写 onConnect/onRecv/onError，
//  完全不感知底层是 epoll 的"就绪通知"还是 IOCP 的"完成通知"。
//
//  使用方式（继承重写虚函数）：
//      class MyClient : public TcpClient {
//          void onConnect(bool ok, const std::string& err) override { ... }
//          void onRecv(const char* data, int len) override { ... }
//          void onError(const std::string& err) override { ... }
//      };
//
//  所有回调都运行在 TcpClient 所绑定的 EventPoller 线程里，
//  调用方不需要自己加锁（前提是不跨线程直接操作 TcpClient 的成员）。
// ================================================================
class TcpClient : public std::enable_shared_from_this<TcpClient> {
public:
    using Ptr = std::shared_ptr<TcpClient>;

    // poller: 这条连接绑定到哪个 EventPoller（通常从 EventPollerPool 取一个）
    explicit TcpClient(EventPoller::Ptr poller);
    virtual ~TcpClient();

    TcpClient(const TcpClient&) = delete;
    TcpClient& operator=(const TcpClient&) = delete;

    // ── 对外接口 ────────────────────────────────────────────────
    // 发起异步连接。host 支持IP或域名，timeout_ms 是连接超时
    // 结果通过 onConnect 回调通知，这个函数本身立即返回，不阻塞
    void connect(const std::string& host, uint16_t port, int timeout_ms = 5000);

    // 异步发送。内部会拷贝一份数据进发送队列，调用后可以立即释放原始buffer
    void send(const char* data, int len);
    void send(const std::string& data);

    // 主动关闭连接（会触发 onError 通知上层，理由是"主动关闭"）
    void close();

    bool isConnected() const { return _connected; }

protected:
    // ── 子类重写这些回调 ────────────────────────────────────────
    virtual void onConnect(bool success, const std::string& err) {}
    virtual void onRecv(const char* data, int len) {}
    virtual void onError(const std::string& err) {}
    // 发送队列从"有数据"变成"空"时触发一次（可选，用于流控判断）
    virtual void onSendComplete() {}

private:
    void doConnect(const sockaddr_in& addr, int timeout_ms);
    void handleConnected(bool ok, const std::string& err);
    void cleanup();

#ifndef _WIN32
    // ── epoll 路径专用 ──────────────────────────────────────────
    void onWritableConnecting(int events);   // 连接阶段：等可写判断connect结果
    void onReadable(int events);             // 已连接：可读事件，循环read
    void onWritableSending(int events);      // 已连接：可写事件，继续flush发送队列
    void tryFlushSendQueue();                // 尝试把发送队列里的数据写出去
#else
    // ── IOCP 路径专用 ───────────────────────────────────────────
    void postNextRecv();                     // 投递下一次异步读（形成"连续读"循环）
    void doSend(std::vector<char> data);     // 实际投递一次 WSASend
#endif

    EventPoller::Ptr _poller;
    sock_t           _fd = INVALID_SOCK;
    std::atomic<bool> _connected{ false };
    std::atomic<bool> _connecting{ false };
    TimerId          _connect_timeout_timer = 0;

    static constexpr int RECV_BUF_SIZE = 64 * 1024;

#ifndef _WIN32
    std::vector<char> _recv_buf;             // epoll: 复用的接收缓冲
    std::deque<std::vector<char>> _send_queue;  // 未发完的数据队列（epoll需要自己管理）
    size_t _send_offset = 0;                 // 队首数据已经发送了多少字节
#else
    std::vector<char> _recv_buf;             // IOCP: 投递读请求时使用的缓冲
    std::atomic<int>  _pending_sends{ 0 };     // 当前还有多少个WSASend正在飞行中
#endif
};