#include "EventPollerPool.h"
#include <iostream>

// ── 单例 ─────────────────────────────────────────────────────────
EventPollerPool& EventPollerPool::Instance() {
    // C++11 起，静态局部变量初始化是线程安全的（魔术静态量）
    static EventPollerPool instance;
    return instance;
}

// ── 构造：创建 N 个 EventPoller 并全部启动 ───────────────────────
EventPollerPool::EventPollerPool(size_t pool_size) {
    /*
    * size_t n = pool_size;
    if (n == 0) {
        n = std::thread::hardware_concurrency();
        if (n == 0) n = 4;   // 极端情况下 hardware_concurrency 返回0，兜底
    }
    */
    size_t n = 5;
    std::cout << "[EventPollerPool] creating " << n << " pollers\n";

    _pollers.reserve(n);
    for (size_t i = 0; i < n; i++) {
        auto poller = std::make_shared<EventPoller>();
        poller->start();
        _pollers.push_back(poller);
    }
}

// ── 析构 ─────────────────────────────────────────────────────────
EventPollerPool::~EventPollerPool() {
    shutdown();
}

// ── getPoller：轮询分配 ──────────────────────────────────────────
EventPoller::Ptr EventPollerPool::getPoller() {
    size_t idx = _next_index.fetch_add(1, std::memory_order_relaxed) % _pollers.size();
    return _pollers[idx];
}

// ── broadcast：给所有Poller投同一个任务 ──────────────────────────
void EventPollerPool::broadcast(Task task) {
    for (auto& poller : _pollers) {
        poller->async(task);   // 注意：task 按值拷贝给每个 Poller，各自独立执行
    }
}

// ── shutdown：停止所有 Poller ─────────────────────────────────────
void EventPollerPool::shutdown() {
    for (auto& poller : _pollers) {
        poller->stop();
    }
    _pollers.clear();
}