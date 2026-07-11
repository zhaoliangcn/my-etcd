#pragma once

#include "raft/transport.h"
#include <thread>
#include <atomic>
#include <mutex>
#include <map>
#include <memory>

namespace myetcd {

// ============================================================
// 简单的二进制序列化工具（网络传输用）
// ============================================================
class BinaryWriter {
public:
    std::vector<uint8_t> buf;

    void WriteU8(uint8_t v);
    void WriteU64(uint64_t v);
    void WriteString(const std::string& s);
    void WriteBytes(const uint8_t* data, size_t len);
};

class BinaryReader {
public:
    const uint8_t* pos;
    const uint8_t* end;

    BinaryReader(const uint8_t* data, size_t len);

    uint8_t  ReadU8();
    uint64_t ReadU64();
    std::string ReadString();
    bool HasMore() const;
};

// ============================================================
// TCP Transport — 基于 TCP 的 Raft RPC 传输层
// ============================================================
class TcpTransport : public Transport {
public:
    TcpTransport(NodeId node_id, const std::string& listen_addr);
    ~TcpTransport() override;

    // 设置对端节点地址
    void SetPeerAddress(NodeId node_id, const std::string& addr);
    void SetPeerAddresses(const std::map<NodeId, std::string>& peers);

    // Transport 接口
    RequestVoteResponse SendRequestVote(NodeId target, const RequestVoteRequest& req) override;
    AppendEntriesResponse SendAppendEntries(NodeId target, const AppendEntriesRequest& req) override;

    void SetRequestVoteHandler(RequestVoteHandler handler) override;
    void SetAppendEntriesHandler(AppendEntriesHandler handler) override;

    void Start() override;
    void Stop() override;

    // 消息类型（用于 TCP 头部）
    enum class MsgType : uint8_t {
        RequestVoteReq    = 1,
        RequestVoteResp   = 2,
        AppendEntriesReq  = 3,
        AppendEntriesResp = 4,
    };

private:
    void ListenerLoop();
    void HandleClient(int client_fd);
    std::vector<uint8_t> ProcessIncomingMsg(const std::vector<uint8_t>& data);

    // 发送并接收（同步 RPC）
    std::vector<uint8_t> SendAndReceive(const std::string& addr, const std::vector<uint8_t>& req_data);

    // 序列化 / 反序列化
    static std::vector<uint8_t> SerializeRequestVote(const RequestVoteRequest& req);
    static RequestVoteResponse  DeserializeRequestVote(const uint8_t* data, size_t len);
    static std::vector<uint8_t> SerializeAppendEntries(const AppendEntriesRequest& req);
    static AppendEntriesResponse DeserializeAppendEntries(const uint8_t* data, size_t len);

    NodeId node_id_;
    std::string listen_addr_;
    std::atomic<bool> running_{false};

    int listen_fd_ = -1;
    std::thread listener_thread_;

    // 对端地址表
    std::map<NodeId, std::string> peer_addrs_;
    std::mutex peer_mu_;

    // 回调
    mutable std::mutex handler_mu_;
    RequestVoteHandler request_vote_handler_;
    AppendEntriesHandler append_entries_handler_;
};

} // namespace myetcd
