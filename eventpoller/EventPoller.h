#pragma once

#include "PlatformSocket.h"

#include <functional>
#include <thread>
#include <mutex>
#include <queue>
#include <map>
#include <memory>
#include <atomic>
#include <condition_variable>
#include <vector>
#include <chrono>

using Task = std::function<void()>;

// ================================================================
//  事件类型（仅 epoll 路径使用，IOCP 路径不需要"就绪"概念）
// ================================================================
enum EventType : int {
    EV_READ = 0x01,
    EV_WRITE = 0x02,
};
using EventCallback = std::function<void(int events)>;

// ================================================================
//  IO 完成回调（仅 IOCP 路径使用）
//  读完成：buf/size 是收到的数据；写完成：buf=nullptr, size=已写字节数
//  ok=false 表示这次 IO 失败（对端断开/出错）
// ================================================================
using IoCallback = std::function<void(bool ok, const char* buf, int size)>;

// ================================================================
//  定时器
//  expire_ms：触发的绝对时间(单位ms，基于 steady_clock)
//  interval_ms：0=一次性；>0=触发后自动按这个间隔重新排期(周期定时器)
//  id：唯一标识，用于 cancelTimer()
// ================================================================
using TimerId = uint64_t;

// 用 shared_ptr<bool> 做取消标记：
//   - TimerTask 自己持有一份
//   - doDelay/doRepeat 返回值通过 _timer_flags[id] 找到这份标记
//   - cancelTimer 只需要把标记置 true，堆里到期检查时跳过即可
//   - 不需要从堆中间删除元素，避免破坏堆结构的复杂操作
struct TimerTask {
    uint64_t            expire_ms;
    uint64_t            interval_ms;   // 0=一次性
    Task                task;
    TimerId             id;
    std::shared_ptr<bool> canceled;    // 指向 true 表示已取消

    bool operator>(const TimerTask& other) const {
        return expire_ms > other.expire_ms;
    }
};

// ================================================================
//  EventPoller
//
//  Linux/macOS  → epoll  ：注册 fd 监听"可读/可写"，应用层主动 read/write
//  Windows      → IOCP   ：投递"读/写请求"，操作系统完成后通知，数据已就位
//
//  两种模型语义不同，提供两组接口：
//    - addEvent/modEvent/delEvent  → epoll 风格（仅 Linux 有效）
//    - postRecv/postSend           → IOCP 风格（仅 Windows 有效）
//
//  定时器接口两平台通用，内部统一用最小堆管理。
// ================================================================
class EventPoller {
public:
    using Ptr = std::shared_ptr<EventPoller>;

    EventPoller();
    ~EventPoller();

    EventPoller(const EventPoller&) = delete;
    EventPoller& operator=(const EventPoller&) = delete;

    void start();
    void stop();

    // ── 任务投递：两个平台通用 ───────────────────────────────────
    void async(Task task);
    void sync(Task task);
    bool isCurrentThread() const;

    // ── 定时器：两个平台通用 ─────────────────────────────────────
    // doDelay：delay_ms 后执行一次
    TimerId doDelay(uint64_t delay_ms, Task task);
    // doRepeat：interval_ms 后首次执行，之后每隔 interval_ms 重复执行
    TimerId doRepeat(uint64_t interval_ms, Task task);
    // 取消一个定时器（一次性或周期性都可以取消）
    void cancelTimer(TimerId id);

#ifndef _WIN32
    // ── epoll 风格接口（仅 Linux）─────────────────────────────
    int addEvent(sock_t fd, int events, EventCallback cb);
    int modEvent(sock_t fd, int events, EventCallback cb);
    int delEvent(sock_t fd);
#else
    // ── IOCP 风格接口（仅 Windows）────────────────────────────
    bool associateSocket(sock_t fd);
    bool postRecv(sock_t fd, char* buf, int len, IoCallback cb);
    bool postSend(sock_t fd, const char* buf, int len, IoCallback cb);
#endif

private:
    void runLoop();
    void wakeup();
    void flushTasks();
    // 专供 doDelay/doRepeat 使用：和 async() 区别在于无条件 wakeup()，
    // 因为插入新定时器会改变下一次 epoll_wait/GetQueuedCompletionStatus 该等多久，
    // 哪怕调用方已经身处 poller 线程内部，也必须唤醒以便下一轮重新计算超时。
    void asyncTimerTask(Task task);

    // ── 定时器内部逻辑 ───────────────────────────────────────────
    // 计算"距离最近一个未取消的定时器还有多少ms"，没有定时器时返回 -1(表示无限等待)
    // 同时会把所有已到期的定时器丢进任务队列执行
    int64_t processTimersAndGetNextDelay();

    static uint64_t nowMs();

    std::mutex       _task_mtx;
    std::queue<Task> _tasks;

    std::thread       _thread;
    std::atomic<bool> _running{ false };
    std::thread::id   _thread_id;

    // ── 定时器堆：只在 poller 线程里访问，不需要加锁 ─────────────
    // greater<> 让 priority_queue 变成小顶堆，堆顶始终是最快到期的任务
    std::priority_queue<TimerTask, std::vector<TimerTask>, std::greater<TimerTask>> _timer_heap;
    std::atomic<TimerId> _timer_id_gen{ 1 };
    // id → 取消标记，doDelay/cancelTimer 通过这个表互相找到对方
    std::map<TimerId, std::shared_ptr<bool>> _timer_flags;

#ifndef _WIN32
    // ── Linux: epoll + pipe ──────────────────────────────────────
    struct FdEntry { int events; EventCallback cb; };
    std::map<sock_t, FdEntry> _fds;   // 只在 poller 线程访问

    int _epoll_fd = -1;
    int _wake[2] = { -1, -1 };
#else
    // ── Windows: IOCP ─────────────────────────────────────────────
    enum class OpType { Recv, Send, Connect, Wake };

    // Connect 完成回调：ok=true 连接成功，ok=false 连接失败（含错误码）
    using ConnectCallback = std::function<void(bool ok, int err_code)>;

    struct IoContext : OVERLAPPED {
        OpType         type;
        sock_t         fd;
        WSABUF         wsabuf;
        IoCallback     cb;          // Recv/Send 用
        ConnectCallback conn_cb;    // Connect 用
    };

    HANDLE     _iocp = nullptr;
    IoContext* _wake_ctx = nullptr;

public:
    // ── IOCP 连接接口 ────────────────────────────────────────────
    // 发起异步连接（内部使用 ConnectEx）。
    // fd 必须先 bind 一个本地地址，且已经 associateSocket 到本 IOCP。
    // 连接完成（成功或失败）后通过 cb 回调通知，回调在 IOCP 线程里执行。
    bool postConnect(sock_t fd, const sockaddr_in& addr, ConnectCallback cb);

private:
#endif
};