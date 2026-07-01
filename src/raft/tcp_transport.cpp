#include "raft/tcp_transport.h"
#include <iostream>
#include <cstring>
#include <sstream>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

namespace myetcd {

// ============================================================
// BinaryWriter
// ============================================================
void BinaryWriter::WriteU8(uint8_t v) {
    buf.push_back(v);
}

void BinaryWriter::WriteU64(uint64_t v) {
    for (int i = 7; i >= 0; --i) {
        buf.push_back(static_cast<uint8_t>(v >> (i * 8)));
    }
}

void BinaryWriter::WriteString(const std::string& s) {
    WriteU64(static_cast<uint64_t>(s.size()));
    for (char c : s) {
        buf.push_back(static_cast<uint8_t>(c));
    }
}

void BinaryWriter::WriteBytes(const uint8_t* data, size_t len) {
    WriteU64(static_cast<uint64_t>(len));
    buf.insert(buf.end(), data, data + len);
}

// ============================================================
// BinaryReader
// ============================================================
BinaryReader::BinaryReader(const uint8_t* data, size_t len)
    : pos(data), end(data + len) {}

uint8_t BinaryReader::ReadU8() {
    if (pos >= end) return 0;
    return *pos++;
}

uint64_t BinaryReader::ReadU64() {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        if (pos >= end) break;
        v = (v << 8) | *pos++;
    }
    return v;
}

std::string BinaryReader::ReadString() {
    uint64_t len = ReadU64();
    if (len > static_cast<size_t>(end - pos)) len = static_cast<size_t>(end - pos);
    std::string s(reinterpret_cast<const char*>(pos), static_cast<size_t>(len));
    pos += len;
    return s;
}

bool BinaryReader::HasMore() const {
    return pos < end;
}

// ============================================================
// TcpTransport
// ============================================================
TcpTransport::TcpTransport(NodeId node_id, const std::string& listen_addr)
    : node_id_(node_id), listen_addr_(listen_addr) {}

TcpTransport::~TcpTransport() {
    Stop();
}

void TcpTransport::SetPeerAddress(NodeId node_id, const std::string& addr) {
    std::lock_guard<std::mutex> lock(peer_mu_);
    peer_addrs_[node_id] = addr;
}

void TcpTransport::SetPeerAddresses(const std::map<NodeId, std::string>& peers) {
    std::lock_guard<std::mutex> lock(peer_mu_);
    peer_addrs_ = peers;
}

void TcpTransport::SetRequestVoteHandler(RequestVoteHandler handler) {
    request_vote_handler_ = std::move(handler);
}

void TcpTransport::SetAppendEntriesHandler(AppendEntriesHandler handler) {
    append_entries_handler_ = std::move(handler);
}

void TcpTransport::Start() {
    running_ = true;

    // 解析监听地址
    size_t colon = listen_addr_.find(':');
    std::string host = (colon != std::string::npos) ? listen_addr_.substr(0, colon) : "0.0.0.0";
    int port = (colon != std::string::npos) ? std::stoi(listen_addr_.substr(colon + 1)) : 2380;

    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        std::cerr << "[TcpTransport] Failed to create listen socket" << std::endl;
        return;
    }

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (bind(listen_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "[TcpTransport] Failed to bind to " << listen_addr_ << std::endl;
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    if (listen(listen_fd_, 16) < 0) {
        std::cerr << "[TcpTransport] Failed to listen" << std::endl;
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    std::cout << "[TcpTransport] Listening on " << listen_addr_ << std::endl;
    listener_thread_ = std::thread(&TcpTransport::ListenerLoop, this);
}

void TcpTransport::Stop() {
    running_ = false;
    if (listen_fd_ >= 0) {
        close(listen_fd_);
        listen_fd_ = -1;
    }
    if (listener_thread_.joinable()) {
        listener_thread_.join();
    }
}

void TcpTransport::ListenerLoop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd_, (sockaddr*)&client_addr, &client_len);

        if (client_fd < 0) {
            if (running_) continue;
            break;
        }

        HandleClient(client_fd);
    }
}

void TcpTransport::HandleClient(int client_fd) {
    // 读取消息头：4 字节消息长度
    uint8_t len_buf[4];
    int n = 0;
    while (n < 4) {
        int r = read(client_fd, len_buf + n, 4 - n);
        if (r <= 0) {
            close(client_fd);
            return;
        }
        n += r;
    }

    uint32_t msg_len = (static_cast<uint32_t>(len_buf[0]) << 24) |
                       (static_cast<uint32_t>(len_buf[1]) << 16) |
                       (static_cast<uint32_t>(len_buf[2]) << 8) |
                       static_cast<uint32_t>(len_buf[3]);

    if (msg_len > 1024 * 1024) { // 最大 1MB
        close(client_fd);
        return;
    }

    // 读取消息体
    std::vector<uint8_t> msg_data(msg_len);
    n = 0;
    while (n < static_cast<int>(msg_len)) {
        int r = read(client_fd, msg_data.data() + n, msg_len - n);
        if (r <= 0) {
            close(client_fd);
            return;
        }
        n += r;
    }

    // 处理消息
    auto resp_data = ProcessIncomingMsg(msg_data);

    // 发送响应
    uint32_t resp_len = static_cast<uint32_t>(resp_data.size());
    uint8_t resp_header[4];
    resp_header[0] = static_cast<uint8_t>(resp_len >> 24);
    resp_header[1] = static_cast<uint8_t>(resp_len >> 16);
    resp_header[2] = static_cast<uint8_t>(resp_len >> 8);
    resp_header[3] = static_cast<uint8_t>(resp_len);

    write(client_fd, resp_header, 4);
    if (!resp_data.empty()) {
        write(client_fd, resp_data.data(), resp_data.size());
    }

    close(client_fd);
}

std::vector<uint8_t> TcpTransport::ProcessIncomingMsg(const std::vector<uint8_t>& data) {
    if (data.empty()) return {};

    BinaryReader reader(data.data(), data.size());
    uint8_t msg_type = reader.ReadU8();

    switch (static_cast<MsgType>(msg_type)) {
    case MsgType::RequestVoteReq: {
        RequestVoteRequest req;
        req.term = reader.ReadU64();
        req.candidate_id = reader.ReadU64();
        req.last_log_index = reader.ReadU64();
        req.last_log_term = reader.ReadU64();

        RequestVoteResponse resp;
        if (request_vote_handler_) {
            resp = request_vote_handler_(req);
        } else {
            resp.term = 0;
            resp.vote_granted = false;
        }

        BinaryWriter w;
        w.WriteU8(static_cast<uint8_t>(MsgType::RequestVoteResp));
        w.WriteU64(resp.term);
        w.WriteU8(resp.vote_granted ? 1 : 0);
        return w.buf;
    }
    case MsgType::AppendEntriesReq: {
        AppendEntriesRequest req;
        req.term = reader.ReadU64();
        req.leader_id = reader.ReadU64();
        req.prev_log_index = reader.ReadU64();
        req.prev_log_term = reader.ReadU64();
        req.leader_commit = reader.ReadU64();

        uint64_t entry_count = reader.ReadU64();
        for (uint64_t i = 0; i < entry_count; ++i) {
            RaftEntry e;
            e.term = reader.ReadU64();
            e.index = reader.ReadU64();
            e.type = static_cast<EventType>(reader.ReadU8());
            e.key = reader.ReadString();
            e.value = reader.ReadString();
            e.lease_id = static_cast<LeaseId>(reader.ReadU64());
            req.entries.push_back(std::move(e));
        }

        AppendEntriesResponse resp;
        if (append_entries_handler_) {
            resp = append_entries_handler_(req);
        } else {
            resp.term = 0;
            resp.success = false;
            resp.last_log_index = 0;
        }

        BinaryWriter w;
        w.WriteU8(static_cast<uint8_t>(MsgType::AppendEntriesResp));
        w.WriteU64(resp.term);
        w.WriteU8(resp.success ? 1 : 0);
        w.WriteU64(resp.last_log_index);
        w.WriteU64(resp.conflict_index);
        w.WriteU64(resp.conflict_term);
        return w.buf;
    }
    default:
        return {};
    }
}

// ============================================================
// 同步 RPC：连接到对端，发送请求，接收响应
// ============================================================
std::vector<uint8_t> TcpTransport::SendAndReceive(const std::string& addr,
                                                  const std::vector<uint8_t>& req_data) {
    size_t colon = addr.find(':');
    if (colon == std::string::npos) return {};

    std::string host = addr.substr(0, colon);
    int port = std::stoi(addr.substr(colon + 1));

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return {};

    // 设置连接超时
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in peer_addr{};
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, host.c_str(), &peer_addr.sin_addr);

    if (connect(fd, (sockaddr*)&peer_addr, sizeof(peer_addr)) < 0) {
        close(fd);
        return {};
    }

    // 发送请求头（4 字节长度）和请求体
    uint32_t req_len = static_cast<uint32_t>(req_data.size());
    uint8_t header[4];
    header[0] = static_cast<uint8_t>(req_len >> 24);
    header[1] = static_cast<uint8_t>(req_len >> 16);
    header[2] = static_cast<uint8_t>(req_len >> 8);
    header[3] = static_cast<uint8_t>(req_len);

    write(fd, header, 4);
    write(fd, req_data.data(), req_data.size());

    // 读取响应头（4 字节长度）
    uint8_t resp_len_buf[4];
    int n = 0;
    while (n < 4) {
        int r = read(fd, resp_len_buf + n, 4 - n);
        if (r <= 0) {
            close(fd);
            return {};
        }
        n += r;
    }

    uint32_t resp_len = (static_cast<uint32_t>(resp_len_buf[0]) << 24) |
                        (static_cast<uint32_t>(resp_len_buf[1]) << 16) |
                        (static_cast<uint32_t>(resp_len_buf[2]) << 8) |
                        static_cast<uint32_t>(resp_len_buf[3]);

    if (resp_len > 1024 * 1024) {
        close(fd);
        return {};
    }

    std::vector<uint8_t> resp_data(resp_len);
    n = 0;
    while (n < static_cast<int>(resp_len)) {
        int r = read(fd, resp_data.data() + n, resp_len - n);
        if (r <= 0) break;
        n += r;
    }

    close(fd);
    return resp_data;
}

// ============================================================
// SendRequestVote
// ============================================================
RequestVoteResponse TcpTransport::SendRequestVote(NodeId target, const RequestVoteRequest& req) {
    RequestVoteResponse resp;
    resp.term = 0;
    resp.vote_granted = false;

    std::string addr;
    {
        std::lock_guard<std::mutex> lock(peer_mu_);
        auto it = peer_addrs_.find(target);
        if (it == peer_addrs_.end()) return resp;
        addr = it->second;
    }

    // 序列化请求
    BinaryWriter w;
    w.WriteU8(static_cast<uint8_t>(MsgType::RequestVoteReq));
    w.WriteU64(req.term);
    w.WriteU64(req.candidate_id);
    w.WriteU64(req.last_log_index);
    w.WriteU64(req.last_log_term);

    auto resp_data = SendAndReceive(addr, w.buf);
    if (resp_data.size() < 2) return resp;

    BinaryReader reader(resp_data.data(), resp_data.size());
    reader.ReadU8(); // msg type
    resp.term = reader.ReadU64();
    resp.vote_granted = reader.ReadU8() != 0;

    return resp;
}

// ============================================================
// SendAppendEntries
// ============================================================
AppendEntriesResponse TcpTransport::SendAppendEntries(NodeId target,
                                                      const AppendEntriesRequest& req) {
    AppendEntriesResponse resp;
    resp.term = 0;
    resp.success = false;
    resp.last_log_index = 0;

    std::string addr;
    {
        std::lock_guard<std::mutex> lock(peer_mu_);
        auto it = peer_addrs_.find(target);
        if (it == peer_addrs_.end()) return resp;
        addr = it->second;
    }

    // 序列化请求
    BinaryWriter w;
    w.WriteU8(static_cast<uint8_t>(MsgType::AppendEntriesReq));
    w.WriteU64(req.term);
    w.WriteU64(req.leader_id);
    w.WriteU64(req.prev_log_index);
    w.WriteU64(req.prev_log_term);
    w.WriteU64(req.leader_commit);
    w.WriteU64(req.entries.size());

    for (const auto& e : req.entries) {
        w.WriteU64(e.term);
        w.WriteU64(e.index);
        w.WriteU8(static_cast<uint8_t>(e.type));
        w.WriteString(e.key);
        w.WriteString(e.value);
        w.WriteU64(static_cast<uint64_t>(e.lease_id));
    }

    auto resp_data = SendAndReceive(addr, w.buf);
    if (resp_data.size() < 2) return resp;

    BinaryReader reader(resp_data.data(), resp_data.size());
    reader.ReadU8(); // msg type
    resp.term = reader.ReadU64();
    resp.success = reader.ReadU8() != 0;
    resp.last_log_index = reader.ReadU64();
    resp.conflict_index = reader.ReadU64();
    resp.conflict_term = reader.ReadU64();

    return resp;
}

} // namespace myetcd
