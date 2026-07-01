#include "server/etcd_server.h"
#include "server/http_handler.h"

#include <iostream>
#include <string>
#include <thread>
#include <map>
#include <sstream>
#include <vector>
#include <cstring>
#include <algorithm>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using socklen_t = int;
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#define SOCKET int
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#endif

namespace myetcd {

// 简单的 HTTP 服务器
class SimpleHttpServer {
public:
    SimpleHttpServer(EtcdServer* server, const std::string& addr, int port)
        : server_(server), addr_(addr), port_(port) {}

    bool Start() {
#ifdef _WIN32
        WSADATA wsa_data;
        if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
            std::cerr << "[HTTP] WSAStartup failed" << std::endl;
            return false;
        }
#endif

        listen_socket_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_socket_ == INVALID_SOCKET) {
            std::cerr << "[HTTP] Failed to create socket" << std::endl;
            return false;
        }

        int opt = 1;
#ifdef _WIN32
        setsockopt(listen_socket_, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
#else
        setsockopt(listen_socket_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        addr.sin_addr.s_addr = INADDR_ANY;
        inet_pton(AF_INET, addr_.c_str(), &addr.sin_addr);

        if (bind(listen_socket_, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
            std::cerr << "[HTTP] Failed to bind to " << addr_ << ":" << port_ << std::endl;
            return false;
        }

        if (listen(listen_socket_, 128) == SOCKET_ERROR) {
            std::cerr << "[HTTP] Failed to listen" << std::endl;
            return false;
        }

        std::cout << "[HTTP] Listening on " << addr_ << ":" << port_ << std::endl;

        running_ = true;
        server_thread_ = std::thread(&SimpleHttpServer::Run, this);
        return true;
    }

    void Stop() {
        running_ = false;
#ifdef _WIN32
        closesocket(listen_socket_);
#else
        close(listen_socket_);
#endif
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
#ifdef _WIN32
        WSACleanup();
#endif
    }

private:
    void Run() {
        while (running_) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            SOCKET client = accept(listen_socket_, (sockaddr*)&client_addr, &client_len);

            if (client == INVALID_SOCKET) {
                if (running_) continue;
                break;
            }

            std::thread(&SimpleHttpServer::HandleClient, this, client).detach();
        }
    }

    void HandleClient(SOCKET client) {
        try {
            HandleClientImpl(client);
        } catch (const std::exception& e) {
            std::cerr << "[ERR] HandleClient exception: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "[ERR] HandleClient unknown exception" << std::endl;
        }
#ifdef _WIN32
        closesocket(client);
#else
        close(client);
#endif
    }

    void HandleClientImpl(SOCKET client) {
        std::string request_str;
        char buffer[4096];
        int total_read = 0;

        // 读取请求头
        while (total_read < 4096 * 4) {
            int n = recv(client, buffer, sizeof(buffer) - 1, 0);
            if (n <= 0) break;
            buffer[n] = '\0';
            request_str += buffer;
            total_read += n;
            if (request_str.find("\r\n\r\n") != std::string::npos) break;
        }

        // 解析请求头以获取 Content-Length
        size_t header_end = request_str.find("\r\n\r\n");
        if (header_end == std::string::npos) {
            return;
        }
        header_end += 4; // 跳过 \r\n\r\n

        // 从请求头中提取 Content-Length
        int content_length = 0;
        std::string header_part = request_str.substr(0, header_end);
        std::istringstream iss(header_part);
        std::string line;
        while (std::getline(iss, line)) {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty()) continue;
            size_t colon = line.find(':');
            if (colon == std::string::npos) continue;
            std::string key = line.substr(0, colon);
            // 大小写不敏感比较
            if (key == "Content-Length" || key == "content-length" || key == "CONTENT-LENGTH") {
                std::string value = line.substr(colon + 1);
                value.erase(0, value.find_first_not_of(" \t"));
                try {
                    content_length = std::stoi(value);
                } catch (const std::invalid_argument&) {
                    content_length = 0;
                } catch (const std::out_of_range&) {
                    content_length = 0;
                }
                break;
            }
        }

        // 读取 body（如果还有未读取的）
        int body_received = static_cast<int>(request_str.size()) - static_cast<int>(header_end);
        if (body_received < 0) body_received = 0;
        while (body_received < content_length && total_read < 4096 * 4) {
            int n = recv(client, buffer, sizeof(buffer) - 1, 0);
            if (n <= 0) break;
            buffer[n] = '\0';
            request_str += buffer;
            total_read += n;
            body_received = static_cast<int>(request_str.size()) - static_cast<int>(header_end);
        }

        HttpRequest req = ParseRequest(request_str);
        HttpResponse resp = HandleRequest(req);

        std::string response_str = BuildResponse(resp);
        send(client, response_str.c_str(), static_cast<int>(response_str.size()), 0);
    }

    HttpRequest ParseRequest(const std::string& raw) {
        HttpRequest req;
        std::istringstream iss(raw);
        std::string line;

        // 请求行
        if (std::getline(iss, line)) {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            std::istringstream line_iss(line);
            line_iss >> req.method >> req.path;
        }

        // 解析 URL 参数
        size_t qpos = req.path.find('?');
        if (qpos != std::string::npos) {
            std::string query = req.path.substr(qpos + 1);
            req.path = req.path.substr(0, qpos);

            std::istringstream qiss(query);
            std::string param;
            while (std::getline(qiss, param, '&')) {
                size_t eq = param.find('=');
                if (eq != std::string::npos) {
                    req.query_params[param.substr(0, eq)] = param.substr(eq + 1);
                }
            }
        }

        // 头部
        while (std::getline(iss, line) && line != "\r" && !line.empty()) {
            if (line.back() == '\r') line.pop_back();
            size_t colon = line.find(':');
            if (colon != std::string::npos) {
                std::string key = line.substr(0, colon);
                std::string value = line.substr(colon + 1);
                // trim
                value.erase(0, value.find_first_not_of(" \t"));
                req.headers[key] = value;
            }
        }

        // Body - 大小写不敏感查找 Content-Length
        int content_length = 0;
        for (const auto& h : req.headers) {
            if (h.first == "Content-Length" || h.first == "content-length" || h.first == "CONTENT-LENGTH") {
                try {
                    content_length = std::stoi(h.second);
                } catch (const std::invalid_argument&) {
                    content_length = 0;
                } catch (const std::out_of_range&) {
                    content_length = 0;
                }
                break;
            }
        }
        if (content_length > 0) {
            req.body = raw.substr(raw.find("\r\n\r\n") + 4, content_length);
        }

        return req;
    }

    HttpResponse HandleRequest(const HttpRequest& req) {
        // 路由分发
        if (req.method == "PUT" && req.path == "/v3/kv/put") {
            return HandlePut(req);
        }
        if (req.method == "POST" && req.path == "/v3/kv/range") {
            return HandleRange(req);
        }
        if (req.method == "POST" && req.path == "/v3/kv/delete") {
            return HandleDelete(req);
        }
        if (req.method == "POST" && req.path == "/v3/watch") {
            return HandleWatch(req);
        }
        if (req.method == "POST" && req.path == "/v3/watch/cancel") {
            return HandleWatchCancel(req);
        }
        if (req.method == "POST" && req.path == "/v3/lease/grant") {
            return HandleLeaseGrant(req);
        }
        if (req.method == "POST" && req.path == "/v3/lease/revoke") {
            return HandleLeaseRevoke(req);
        }
        if (req.method == "POST" && req.path == "/v3/lease/keepalive") {
            return HandleLeaseKeepAlive(req);
        }
        if (req.method == "GET" && req.path == "/v3/cluster/info") {
            return server_->ClusterInfo();
        }
        if (req.method == "GET" && req.path == "/") {
            return HandleIndex();
        }

        HttpResponse resp;
        resp.SetError(404, "not found: " + req.path);
        return resp;
    }

    std::string GetParam(const HttpRequest& req, const std::string& name) {
        auto it = req.query_params.find(name);
        return it != req.query_params.end() ? it->second : "";
    }

    HttpResponse HandlePut(const HttpRequest& req) {
        std::string key = GetParam(req, "key");
        std::string value = req.body;
        LeaseId lease_id = 0;
        auto it = req.query_params.find("lease");
        if (it != req.query_params.end()) {
            try {
                lease_id = std::stoll(it->second);
            } catch (const std::invalid_argument&) {
                lease_id = 0;
            } catch (const std::out_of_range&) {
                lease_id = 0;
            }
        }
        if (key.empty()) {
            HttpResponse resp;
            resp.SetError(400, "key is required");
            return resp;
        }
        return server_->Put(key, value, lease_id);
    }

    HttpResponse HandleRange(const HttpRequest& req) {
        std::string key = GetParam(req, "key");
        std::string range_end = GetParam(req, "range_end");
        if (key.empty()) {
            HttpResponse resp;
            resp.SetError(400, "key is required");
            return resp;
        }
        return server_->Range(key, range_end);
    }

    HttpResponse HandleDelete(const HttpRequest& req) {
        std::string key = GetParam(req, "key");
        if (key.empty()) {
            HttpResponse resp;
            resp.SetError(400, "key is required");
            return resp;
        }
        return server_->Delete(key);
    }

    HttpResponse HandleWatch(const HttpRequest& req) {
        std::string key = GetParam(req, "key");
        bool prefix = GetParam(req, "prefix") == "true";
        if (key.empty()) {
            HttpResponse resp;
            resp.SetError(400, "key is required");
            return resp;
        }
        return server_->Watch(key, 0, prefix);
    }

    HttpResponse HandleWatchCancel(const HttpRequest& req) {
        LeaseId watch_id = 0;
        auto it = req.query_params.find("ID");
        if (it != req.query_params.end()) {
            try {
                watch_id = std::stoll(it->second);
            } catch (const std::invalid_argument&) {
                watch_id = 0;
            } catch (const std::out_of_range&) {
                watch_id = 0;
            }
        }
        if (watch_id <= 0) {
            HttpResponse resp;
            resp.SetError(400, "ID is required");
            return resp;
        }
        return server_->WatchCancel(watch_id);
    }

    HttpResponse HandleLeaseGrant(const HttpRequest& req) {
        int64_t ttl = 0;
        auto it = req.query_params.find("TTL");
        if (it != req.query_params.end()) {
            try {
                ttl = std::stoll(it->second);
            } catch (const std::invalid_argument&) {
                ttl = 0;
            } catch (const std::out_of_range&) {
                ttl = 0;
            }
        }
        if (ttl <= 0) {
            HttpResponse resp;
            resp.SetError(400, "TTL is required and must be positive");
            return resp;
        }
        return server_->LeaseGrant(ttl);
    }

    HttpResponse HandleLeaseRevoke(const HttpRequest& req) {
        LeaseId id = 0;
        auto it = req.query_params.find("ID");
        if (it != req.query_params.end()) {
            try {
                id = std::stoll(it->second);
            } catch (const std::invalid_argument&) {
                id = 0;
            } catch (const std::out_of_range&) {
                id = 0;
            }
        }
        if (id <= 0) {
            HttpResponse resp;
            resp.SetError(400, "ID is required");
            return resp;
        }
        return server_->LeaseRevoke(id);
    }

    HttpResponse HandleLeaseKeepAlive(const HttpRequest& req) {
        LeaseId id = 0;
        auto it = req.query_params.find("ID");
        if (it != req.query_params.end()) {
            try {
                id = std::stoll(it->second);
            } catch (const std::invalid_argument&) {
                id = 0;
            } catch (const std::out_of_range&) {
                id = 0;
            }
        }
        if (id <= 0) {
            HttpResponse resp;
            resp.SetError(400, "ID is required");
            return resp;
        }
        return server_->LeaseKeepAlive(id);
    }

    HttpResponse HandleIndex() {
        HttpResponse resp;
        resp.headers["Content-Type"] = "text/html; charset=utf-8";
        resp.body = R"(<!DOCTYPE html>
<html>
<head><title>my-etcd</title></head>
<body>
<h1>my-etcd</h1>
<p>A distributed key-value store built with C++17</p>
<h2>API Endpoints</h2>
<ul>
  <li>PUT /v3/kv/put?key=xxx - Put a key-value pair</li>
  <li>POST /v3/kv/range?key=xxx - Range query</li>
  <li>POST /v3/kv/delete?key=xxx - Delete a key</li>
  <li>POST /v3/watch?key=xxx - Watch a key</li>
  <li>POST /v3/lease/grant?TTL=60 - Grant a lease</li>
  <li>POST /v3/lease/revoke?ID=1 - Revoke a lease</li>
  <li>POST /v3/lease/keepalive?ID=1 - Keep a lease alive</li>
  <li>GET /v3/cluster/info - Cluster information</li>
</ul>
</body>
</html>)";
        return resp;
    }

    std::string BuildResponse(const HttpResponse& resp) {
        std::ostringstream oss;
        oss << "HTTP/1.1 " << resp.status_code << " ";
        switch (resp.status_code) {
            case 200: oss << "OK"; break;
            case 400: oss << "Bad Request"; break;
            case 404: oss << "Not Found"; break;
            case 500: oss << "Internal Server Error"; break;
            case 503: oss << "Service Unavailable"; break;
            default: oss << "Unknown"; break;
        }
        oss << "\r\n";

        auto headers = resp.headers;
        headers["Content-Length"] = std::to_string(resp.body.size());
        headers["Connection"] = "close";
        headers["Server"] = "my-etcd/1.0";

        for (const auto& [k, v] : headers) {
            oss << k << ": " << v << "\r\n";
        }
        oss << "\r\n";
        oss << resp.body;

        return oss.str();
    }

    EtcdServer* server_;
    std::string addr_;
    int port_;
    SOCKET listen_socket_ = INVALID_SOCKET;
    std::thread server_thread_;
    std::atomic<bool> running_{false};
};

} // namespace myetcd

// 打印帮助信息
void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  --name <name>             Node name (default: default)\n"
              << "  --data-dir <path>         Data directory (default: ./data)\n"
              << "  --listen-addr <addr>      Client listen address (default: 0.0.0.0:2379)\n"
              << "  --listen-peer-addr <addr> Peer listen address (default: 0.0.0.0:2380)\n"
              << "  --initial-cluster <str>   Initial cluster config\n"
              << "  --help                    Show this help\n"
              << std::endl;
}

int main(int argc, char* argv[]) {
    using namespace myetcd;

    ClusterConfig config;

    // 解析命令行参数
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--name" && i + 1 < argc) {
            config.name = argv[++i];
        } else if (arg == "--data-dir" && i + 1 < argc) {
            config.data_dir = argv[++i];
        } else if (arg == "--listen-addr" && i + 1 < argc) {
            config.listen_addr = argv[++i];
        } else if (arg == "--listen-peer-addr" && i + 1 < argc) {
            config.listen_peer_addr = argv[++i];
        } else if (arg == "--help") {
            PrintUsage(argv[0]);
            return 0;
        }
    }

    std::cout << "============================================" << std::endl;
    std::cout << "  my-etcd - Distributed Key-Value Store" << std::endl;
    std::cout << "  Version 1.0.0 (C++17)" << std::endl;
    std::cout << "============================================" << std::endl;

    // 创建并启动服务
    EtcdServer server(config);

    if (!server.Start()) {
        std::cerr << "Failed to start server" << std::endl;
        return 1;
    }

    // 解析监听地址
    std::string listen_addr = config.listen_addr;
    size_t colon = listen_addr.find(':');
    std::string host = "0.0.0.0";
    int port = 2379;
    if (colon != std::string::npos) {
        host = listen_addr.substr(0, colon);
        port = std::stoi(listen_addr.substr(colon + 1));
    }

    // 启动 HTTP 服务器
    SimpleHttpServer http_server(&server, host, port);
    if (!http_server.Start()) {
        std::cerr << "Failed to start HTTP server" << std::endl;
        return 1;
    }

    std::cout << "Press Ctrl+C to stop..." << std::endl;

    // 主循环等待信号
    while (server.IsRunning()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    http_server.Stop();
    server.Stop();

    return 0;
}