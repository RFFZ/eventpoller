#pragma once

#include "EventPoller.h"
#include <vector>
#include <thread>
#include <atomic>
#include <memory>

// ================================================================
//  EventPollerPool
//
//  职责：
//    1. 启动时创建 N 个 EventPoller（N 默认=CPU核数）
//    2. 提供 getPoller() 做负载均衡分配（轮询）
//    3. 提供 for_each，把同一个任务广播给所有 Poller
//
//  典型用法（对应 ZLMediaKit 的 EventPollerPool::Instance()）：
//
//    auto& pool = EventPollerPool::Instance();
//    auto poller = pool.getPoller();      // 给新连接/新流分配一个Poller
//    poller->doRepeat(frame_interval_ms, sendFrameTask);
//
//  单例：整个进程通常只需要一个 Pool，所有推流连接共享。
// ================================================================
class EventPollerPool {
public:
    // ── 单例访问 ─────────────────────────────────────────────────
    static EventPollerPool& Instance();

    // 禁止拷贝
    EventPollerPool(const EventPollerPool&) = delete;
    EventPollerPool& operator=(const EventPollerPool&) = delete;

    // ── 获取一个 Poller（轮询分配，线程安全）────────────────────
    EventPoller::Ptr getPoller();

    // ── Pool 内 Poller 数量 ──────────────────────────────────────
    size_t size() const { return _pollers.size(); }

    // ── 把同一个任务广播给所有 Poller 执行（异步，不等待）───────
    void broadcast(Task task);

    // ── 优雅退出：停止所有 Poller ────────────────────────────────
    void shutdown();

private:
    // 私有构造：池子在首次 Instance() 调用时创建
    // pool_size=0 表示自动取 CPU 核数
    explicit EventPollerPool(size_t pool_size = 0);
    ~EventPollerPool();

    std::vector<EventPoller::Ptr> _pollers;
    std::atomic<size_t>           _next_index{ 0 };
};