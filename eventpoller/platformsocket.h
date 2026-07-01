#pragma once

#include <string>
#include <cstring>

// ================================================================
//  平台差异全部隔离在这一个头文件里
//  上层代码只包含这个文件，不直接写 #ifdef _WIN32
// ================================================================

#ifdef _WIN32
// ── Windows ──────────────────────────────────────────────────────
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX   // 防止 windows.h 定义 min/max 宏，污染 std::min/std::max
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>    // LPFN_CONNECTEX / WSAID_CONNECTEX 在这里声明
#include <windows.h>
#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "mswsock.lib")

// Windows 用 SOCKET 类型，Linux 用 int
// 统一用 sock_t
using sock_t = SOCKET;
#define INVALID_SOCK  INVALID_SOCKET
#define SOCK_ERRNO    WSAGetLastError()

// Windows 没有 O_NONBLOCK，用 ioctlsocket 实现
inline int setNonBlock(sock_t fd) {
    u_long mode = 1;
    return ioctlsocket(fd, FIONBIO, &mode) == 0 ? 0 : -1;
}

// Windows 关闭 socket 用 closesocket
inline void closeSocket(sock_t fd) { closesocket(fd); }

// Windows 的 select 用 fd_set，和 Linux 一样，但 FD_SETSIZE 默认 64
// select 第一个参数在 Windows 上被忽略，传 0 即可
#define SELECT_NFDS(maxfd)  0

// Windows 下 WSAEWOULDBLOCK 对应 Linux 的 EAGAIN/EWOULDBLOCK
#define ERR_WOULDBLOCK  WSAEWOULDBLOCK
#define ERR_INPROGRESS  WSAEWOULDBLOCK   // Windows connect 非阻塞返回这个

// ── Windows 用 loopback socketpair 模拟 pipe ─────────────────────
// Linux 有 pipe()，Windows 没有，用两个本地 TCP socket 代替
inline int socketpair_tcp(sock_t sv[2]) {
    sv[0] = INVALID_SOCKET;
    sv[1] = INVALID_SOCKET;

    sock_t listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener == INVALID_SOCKET) return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;   // 让系统分配端口

    if (bind(listener, (sockaddr*)&addr, sizeof(addr)) != 0) goto fail;
    if (listen(listener, 1) != 0) goto fail;

    {   // 获取实际分配的端口
        int len = sizeof(addr);
        if (getsockname(listener, (sockaddr*)&addr, &len) != 0) goto fail;
    }

    sv[0] = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sv[0] == INVALID_SOCKET) goto fail;
    if (connect(sv[0], (sockaddr*)&addr, sizeof(addr)) != 0) goto fail;

    sv[1] = accept(listener, nullptr, nullptr);
    if (sv[1] == INVALID_SOCKET) goto fail;

    closesocket(listener);
    setNonBlock(sv[0]);
    setNonBlock(sv[1]);
    return 0;

fail:
    closesocket(listener);
    if (sv[0] != INVALID_SOCKET) closesocket(sv[0]);
    sv[0] = sv[1] = INVALID_SOCKET;
    return -1;
}

// 统一 wakeup pipe 的读写接口
inline int  pipeWrite(sock_t fd, const char* buf, int len) { return send(fd, buf, len, 0); }
inline int  pipeRead(sock_t fd, char* buf, int len) { return recv(fd, buf, len, 0); }
inline void pipeClose(sock_t fd) { closesocket(fd); }

// WinSock 初始化（main 里调一次）
struct WsaInit {
    WsaInit() { WSADATA d; WSAStartup(MAKEWORD(2, 2), &d); }
    ~WsaInit() { WSACleanup(); }
};

// ── ConnectEx 函数指针：必须运行时通过 WSAIoctl 获取 ─────────────
// ConnectEx 不在 ws2_32.lib 的导出表里，是 Winsock 扩展函数，
// 必须先创建一个 socket，再用 WSAIoctl(SIO_GET_EXTENSION_FUNCTION_POINTER) 查询。
// 用 IOCP 发起异步 connect 必须用 ConnectEx（普通 connect 不支持 OVERLAPPED）。
inline LPFN_CONNECTEX getConnectExPtr(sock_t fd) {
    GUID guid = WSAID_CONNECTEX;
    LPFN_CONNECTEX fn = nullptr;
    DWORD bytes = 0;
    WSAIoctl(fd, SIO_GET_EXTENSION_FUNCTION_POINTER,
        &guid, sizeof(guid), &fn, sizeof(fn), &bytes, nullptr, nullptr);
    return fn;
}

#else
// ── Linux / macOS ────────────────────────────────────────────────
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <cerrno>

using sock_t = int;
#define INVALID_SOCK  (-1)
#define SOCK_ERRNO    errno

inline int setNonBlock(sock_t fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1 ? -1 : 0;
}
inline void closeSocket(sock_t fd) { close(fd); }

#define SELECT_NFDS(maxfd)  ((maxfd) + 1)
#define ERR_WOULDBLOCK  EAGAIN
#define ERR_INPROGRESS  EINPROGRESS

// Linux 直接用 pipe2
inline int socketpair_tcp(sock_t sv[2]) {
    int pfd[2];
    if (pipe2(pfd, O_CLOEXEC) != 0) return -1;
    setNonBlock(pfd[0]);
    setNonBlock(pfd[1]);
    sv[0] = pfd[0];
    sv[1] = pfd[1];
    return 0;
}

inline int  pipeWrite(sock_t fd, const char* buf, int len) { return (int)write(fd, buf, len); }
inline int  pipeRead(sock_t fd, char* buf, int len) { return (int)read(fd, buf, len); }
inline void pipeClose(sock_t fd) { close(fd); }

// Linux 不需要 WsaInit，给一个空壳保持调用侧一致
struct WsaInit {};

#endif // _WIN32

// ================================================================
//  以下函数两个平台通用：用 sockaddr_in / inet_pton 等标准 BSD socket API
//  Windows 和 Linux 这部分接口完全一致，不需要 #ifdef
// ================================================================

// 域名/IP 字符串 → sockaddr_in。支持直接传IP，也支持域名（内部走getaddrinfo）
inline bool resolveAddress(const std::string& host, uint16_t port, sockaddr_in& out) {
    memset(&out, 0, sizeof(out));
    out.sin_family = AF_INET;
    out.sin_port = htons(port);

    // 先尝试当成纯IP解析（最常见情况，不需要DNS查询）
    if (inet_pton(AF_INET, host.c_str(), &out.sin_addr) == 1) {
        return true;
    }

    // 不是纯IP，走域名解析
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    if (getaddrinfo(host.c_str(), nullptr, &hints, &result) != 0 || !result) {
        return false;
    }
    out.sin_addr = ((sockaddr_in*)result->ai_addr)->sin_addr;
    freeaddrinfo(result);
    return true;
}

// 创建一个非阻塞 TCP socket
inline sock_t createTcpSocket() {
    sock_t fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd != INVALID_SOCK) setNonBlock(fd);
    return fd;
}