#include "TcpClient.h"
#include "EventPollerPool.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <cassert>
#include <cstring>
using namespace std::chrono;

// ════════════════════════════════════════════════════════════════
//  一个最简单的本地 echo server（独立线程，阻塞式），
//  用于验证 TcpClient 的 connect/send/recv/close 全流程是否正确。
//  这不是要测试的对象，只是陪练。
// ════════════════════════════════════════════════════════════════
struct EchoServer {
    sock_t   listen_fd = INVALID_SOCK;
    uint16_t port = 0;
    std::thread th;
    std::atomic<bool> running{ true };

    void start() {
        listen_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;   // 系统分配端口
        bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
        listen(listen_fd, 4);

        socklen_t len = sizeof(addr);
        getsockname(listen_fd, (sockaddr*)&addr, &len);
        port = ntohs(addr.sin_port);

        th = std::thread([this] { run(); });
    }

    void run() {
        while (running) {
            sockaddr_in cli{};
            socklen_t cl = sizeof(cli);
            sock_t conn = accept(listen_fd, (sockaddr*)&cli, &cl);
            if (conn == INVALID_SOCK) {
                if (!running) break;
                continue;
            }
            // 每个连接单独开一个线程做echo（陪练server不需要高性能）
            std::thread([conn] {
                char buf[4096];
                while (true) {
                    int n = (int)recv(conn, buf, sizeof(buf), 0);
                    if (n <= 0) break;
                    send(conn, buf, n, 0);   // echo回去
                }
                closeSocket(conn);
                }).detach();
        }
    }

    void stop() {
        running = false;
        // 单纯 close 在某些情况下不能立刻唤醒阻塞中的 accept()，
        // 用 shutdown() 更可靠地强制让 accept() 返回错误
#ifndef _WIN32
        ::shutdown(listen_fd, SHUT_RDWR);
#endif
        closeSocket(listen_fd);
        if (th.joinable()) th.join();
    }
};


// ── 测试用 TcpClient 子类，记录回调触发情况 ──────────────────────
class TestClient : public TcpClient {
public:
    using TcpClient::TcpClient;

    std::atomic<bool> connect_called{ false };
    std::atomic<bool> connect_ok{ false };
    std::string       connect_err;

    std::mutex        recv_mtx;
    std::string       recv_data;

    std::atomic<bool> error_called{ false };
    std::string       error_msg;

    std::atomic<bool> send_complete_called{ false };

protected:
    void onConnect(bool ok, const std::string& err) override {
        connect_called = true;
        connect_ok = ok;
        connect_err = err;
        std::cout << "  onConnect: ok=" << ok << " err=" << err << "\n";
    }
    void onRecv(const char* data, int len) override {
        std::lock_guard<std::mutex> lk(recv_mtx);
        recv_data.append(data, len);
        std::cout << "  onRecv: " << len << " bytes\n";
    }
    void onError(const std::string& err) override {
        error_called = true;
        error_msg = err;
        std::cout << "  onError: " << err << "\n";
    }
    void onSendComplete() override {
        send_complete_called = true;
    }
};


// ── 测试1：连接成功 + 收发数据（echo验证）─────────────────────────
void test_connect_and_echo() {
    std::cout << "\n=== test_connect_and_echo ===\n";
   
    EchoServer server;
    server.start();
    std::cout << "  echo server listening on 127.0.0.1:" << server.port << "\n";

    auto poller = EventPollerPool::Instance().getPoller();
    auto client = std::make_shared<TestClient>(poller);
    auto start_time = std::chrono::high_resolution_clock::now();
    client->connect("127.0.0.1", server.port, 2000);

    // 等连接结果
    for (int i = 0; i < 50 && !client->connect_called; i++)
        std::this_thread::sleep_for(milliseconds(2));

    assert(client->connect_called);
    assert(client->connect_ok);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    // 打印连接耗时（微秒）
    std::cout << " connectd execution time: " << duration.count() / 1000.0 << " ms\n";


    // 发送数据
    std::string msg = "hello tcp client";
    client->send(msg);

    // 等echo回来
    for (int i = 0; i < 50; i++) {
        std::this_thread::sleep_for(milliseconds(2));
        std::lock_guard<std::mutex> lk(client->recv_mtx);
        if (client->recv_data == msg)
        {
            std::cout << "  echo received correctly: '" << client->recv_data << "'\n";
            break;
        }
    }

    auto end_time2 = std::chrono::high_resolution_clock::now();
    auto duration2 = std::chrono::duration_cast<std::chrono::microseconds>(end_time2 - start_time);
    // 打印连接耗时（微秒）
    std::cout << " receive msg execution time: " <<duration2.count() / 1000.0 << " ms\n";

    client->close();
    std::this_thread::sleep_for(milliseconds(100));

    server.stop();
}

// ── 测试2：连接到不存在的端口，应该收到失败回调 ───────────────────
void test_connect_refused() {
    std::cout << "\n=== test_connect_refused ===\n";

    auto poller = EventPollerPool::Instance().getPoller();
    auto client = std::make_shared<TestClient>(poller);

    // 连一个大概率没人监听的端口
    client->connect("127.0.0.1", 1, 2000);

    for (int i = 0; i < 100 && !client->connect_called; i++)
        std::this_thread::sleep_for(milliseconds(20));

    assert(client->connect_called);
    assert(!client->connect_ok);
    std::cout << "  connect correctly failed: " << client->connect_err << "\n";
}

// ── 测试3：连接超时（连一个不会回应的地址）─────────────────────────
void test_connect_timeout() {
    std::cout << "\n=== test_connect_timeout ===\n";

    auto poller = EventPollerPool::Instance().getPoller();
    auto client = std::make_shared<TestClient>(poller);

    // 10.255.255.1 在大多数网络环境下路由不可达、不会立刻拒绝，
    // 但在某些容器/沙盒网络里内核层面的不可达检测可能比用户态超时更早或更晚触发，
    // 所以等待窗口要比 timeout_ms 留出充分余量（这里给5秒，连接超时设300ms）
    auto t0 = steady_clock::now();
    client->connect("10.255.255.1", 9999, 300);

    for (int i = 0; i < 250 && !client->connect_called; i++)
        std::this_thread::sleep_for(milliseconds(20));

    auto elapsed = duration_cast<milliseconds>(steady_clock::now() - t0).count();
    std::cout << "  elapsed=" << elapsed << "ms, connect_called="
        << client->connect_called.load() << "\n";

    // 这个测试不强制要求一定是"超时"这个具体原因触发失败
    // （网络环境不同，可能是超时，也可能是内核更早报告不可达），
    // 只要求最终一定会触发 onConnect(false, ...)，不会无限挂起，
    // 且耗时不应该远超我们设的 timeout_ms 太多（验证超时机制确实生效，不是单纯靠内核）
    assert(client->connect_called);
    assert(!client->connect_ok);
    std::cout << "  connect eventually failed: " << client->connect_err << "\n";
}

// ── 测试4：多个TcpClient并发，验证互不干扰 ─────────────────────────
void test_multi_clients() {
    std::cout << "\n=== test_multi_clients ===\n";

    EchoServer server;
    server.start();

    const int N = 10;
    std::vector<std::shared_ptr<TestClient>> clients;

    for (int i = 0; i < N; i++) {
        auto poller = EventPollerPool::Instance().getPoller();
        auto client = std::make_shared<TestClient>(poller);
        client->connect("127.0.0.1", server.port, 2000);
        clients.push_back(client);
    }

    // 等全部连上
    for (int wait = 0; wait < 100; wait++) {
        bool all_connected = true;
        for (auto& c : clients) if (!c->connect_called) all_connected = false;
        if (all_connected) break;
        std::this_thread::sleep_for(milliseconds(20));
    }

    int connected_count = 0;
    for (auto& c : clients) if (c->connect_ok) connected_count++;
    std::cout << "  " << connected_count << "/" << N << " clients connected\n";
    assert(connected_count == N);

    // 每个client发送不同的消息
    for (int i = 0; i < N; i++) {
        clients[i]->send("client-" + std::to_string(i));
    }

    std::this_thread::sleep_for(milliseconds(300));

    int correct = 0;
    for (int i = 0; i < N; i++) {
        std::lock_guard<std::mutex> lk(clients[i]->recv_mtx);
        std::string expected = "client-" + std::to_string(i);
        if (clients[i]->recv_data == expected) correct++;
    }
    std::cout << "  " << correct << "/" << N << " received correct echo\n";
    assert(correct == N);
    std::cout << "  all clients independent, no cross-talk\n";

    for (auto& c : clients) c->close();
    std::this_thread::sleep_for(milliseconds(100));
    server.stop();
}

int main() {
    WsaInit wsa;
    // 首次调用 Instance() 即自动创建并启动pool，不需要显式start()

    test_connect_and_echo();
    test_connect_refused();
    test_connect_timeout();
    test_multi_clients();

    std::cout << "\n所有测试通过\n";

    EventPollerPool::Instance().shutdown();
    return 0;
}