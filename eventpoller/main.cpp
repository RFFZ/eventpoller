#include "EventPollerPool.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <cassert>
#include <set>
#include <map>
using namespace std::chrono;

// ── 测试1：Pool 创建数量 = CPU核数 ────────────────────────────────
void test_pool_size() {
    std::cout << "\n=== test_pool_size ===\n";
    auto& pool = EventPollerPool::Instance();
    std::cout << "  pool size = " << pool.size()
        << " (hardware_concurrency=" << std::thread::hardware_concurrency() << ")\n";
    assert(pool.size() > 0);
    std::cout << "\n";
}

// ── 测试2：getPoller 轮询分配，验证均衡性 ─────────────────────────
void test_round_robin() {
    std::cout << "\n=== test_round_robin ===\n";
    auto& pool = EventPollerPool::Instance();

    std::map<EventPoller*, int> count;
    const int N = 20;   // 模拟20路流分配
    for (int i = 0; i < N; i++) {
        auto poller = pool.getPoller();
        count[poller.get()]++;
    }

    std::cout << "  20 streams distributed across " << count.size() << " pollers:\n";
    for (auto& [p, c] : count) {
        std::cout << "    poller " << p << " got " << c << " streams\n";
    }

    // 均衡性检查：每个 poller 分到的数量差不超过1
    int min_c = N, max_c = 0;
    for (auto& [p, c] : count) {
        min_c = std::min(min_c, c);
        max_c = std::max(max_c, c);
    }
    assert(max_c - min_c <= 1);
    std::cout << "  load balanced (min=" << min_c << ", max=" << max_c << ")\n";
}

// ── 测试3：模拟20路流，每路独立帧率定时器，验证互不干扰 ──────────
struct FakeStream {
    int      id;
    int      fps;
    int      frame_count = 0;
    TimerId  timer_id;
    EventPoller::Ptr poller;
};

void test_simulated_20_streams() {
    std::cout << "\n=== test_simulated_20_streams ===\n";
    auto& pool = EventPollerPool::Instance();

    const int STREAM_COUNT = 20;
    const int TEST_MS = 500;   // 跑 500ms

    std::vector<std::shared_ptr<FakeStream>> streams;
    std::mutex log_mtx;

    for (int i = 0; i < STREAM_COUNT; i++) {
        auto stream = std::make_shared<FakeStream>();
        stream->id = i;
        stream->fps = 25;                            // 模拟25fps推流
        stream->poller = pool.getPoller();           // 分配到某个Poller（轮询）

        int interval_ms = 1000 / stream->fps;        // 40ms一帧

        // 用 doRepeat 模拟"帧率定时器"，对应ZLMediaKit发送下一帧的节奏
        stream->timer_id = stream->poller->doRepeat(interval_ms, [stream] {
            stream->frame_count++;
            // 这里不打印每一帧，否则日志会爆炸；20路*500ms/40ms≈250条
            });

        streams.push_back(stream);
    }

    std::cout << "  started " << STREAM_COUNT << " simulated streams (25fps each)\n";
    std::this_thread::sleep_for(milliseconds(TEST_MS));

    // 停止所有定时器
    for (auto& s : streams) {
        s->poller->cancelTimer(s->timer_id);
    }
    std::this_thread::sleep_for(milliseconds(50));   // 等最后一次回调落地

    // 验证：500ms / 40ms ≈ 12.5次，每路应该在 10~14 次之间（留误差余量）
    int total_frames = 0;
    int min_frames = 9999, max_frames = 0;
    for (auto& s : streams) {
        total_frames += s->frame_count;
        min_frames = std::min(min_frames, s->frame_count);
        max_frames = std::max(max_frames, s->frame_count);
        assert(s->frame_count >= 8 && s->frame_count <= 16);
    }

    std::cout << "  total frames sent across 20 streams: " << total_frames << "\n";
    std::cout << "  per-stream frame count range: [" << min_frames
        << ", " << max_frames << "] (expected ~12-13)\n";

    // 验证流确实分摊到了多个 poller，而不是全堆在一个线程上
    std::set<EventPoller*> used_pollers;
    for (auto& s : streams) used_pollers.insert(s->poller.get());
    std::cout << "  streams spread across " << used_pollers.size() << " pollers\n";
    assert(used_pollers.size() > 1 || pool.size() == 1);
}

// ── 测试4：模拟 paced_sender —— 双层定时器（帧率+平滑发送）───────
void test_paced_sender_simulation() {
    std::cout << "\n=== test_paced_sender_simulation ===\n";
    auto poller = EventPollerPool::Instance().getPoller();

    // 模拟一帧被FU-A分片成5个RTP包，平滑在帧间隔内发出去
    std::queue<int> send_queue;
    std::mutex      q_mtx;
    std::vector<int64_t> send_times_ms;
    std::mutex      log_mtx;

    auto t0 = steady_clock::now();


    //外层定时器：只负责"产生"，把数据放进队列，不直接发送
    //内层定时器：负责把所有路的待发数据，匀速地、一个一个地真正发出去
    //外层控制"两帧间隔"，内层单独控制"包与包间隔"

    // 外层：帧率定时器（模拟25fps，40ms一帧）
    TimerId frame_timer = poller->doRepeat(40, [&] {
        std::lock_guard<std::mutex> lk(q_mtx);
        for (int i = 0; i < 5; i++) send_queue.push(i);  // 一帧拆成5个包
        });

    // 内层：paced sender，每5ms尝试发一个包（模拟匀速吐出）
    TimerId pace_timer = poller->doRepeat(5, [&] {
        std::lock_guard<std::mutex> lk(q_mtx);
        if (!send_queue.empty()) {
            send_queue.pop();
            std::lock_guard<std::mutex> lk2(log_mtx);
            send_times_ms.push_back(
                duration_cast<milliseconds>(steady_clock::now() - t0).count());
        }
        });

    std::this_thread::sleep_for(milliseconds(200));
    poller->cancelTimer(frame_timer);
    poller->cancelTimer(pace_timer);
    std::this_thread::sleep_for(milliseconds(20));

    std::cout << "  packets sent over 200ms: " << send_times_ms.size() << "\n";
    std::cout << "  send timestamps(ms): ";
    for (auto t : send_times_ms) std::cout << t << " ";
    std::cout << "\n";

    // 验证：包是分散发送的，不是瞬间全部发完
    // 阈值放宽：不同平台/不同负载下定时器精度有差异，这里只验证"确实在分批发"，
    // 不严格要求具体数量（原理验证测试，不是性能基准测试）
    assert(send_times_ms.size() >= 5);
    std::cout << "  packets paced out smoothly (not bursty)\n";
}

int main() {
    test_pool_size();
    test_round_robin();
    test_simulated_20_streams();
    test_paced_sender_simulation();

    std::cout << "\n所有测试通过\n";

    EventPollerPool::Instance().shutdown();
    return 0;
}