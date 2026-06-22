#pragma once

#include "common/types.h"
#include <functional>
#include <memory>

namespace myetcd {

// Raft RPC 请求/响应
struct RequestVoteRequest {
    Term term;
    NodeId candidate_id;
    Index last_log_index;
    Term last_log_term;
};

struct RequestVoteResponse {
    Term term;
    bool vote_granted;
};

struct AppendEntriesRequest {
    Term term;
    NodeId leader_id;
    Index prev_log_index;
    Term prev_log_term;
    std::vector<RaftEntry> entries;
    Index leader_commit;
};

struct AppendEntriesResponse {
    Term term;
    bool success;
    Index last_log_index;
};

// 传输层抽象接口
class Transport {
public:
    virtual ~Transport() = default;

    // 发送 RequestVote RPC
    virtual void SendRequestVote(NodeId target, const RequestVoteRequest& req) = 0;

    // 发送 AppendEntries RPC
    virtual void SendAppendEntries(NodeId target, const AppendEntriesRequest& req) = 0;

    // 设置回调
    using RequestVoteHandler = std::function<RequestVoteResponse(const RequestVoteRequest&)>;
    using AppendEntriesHandler = std::function<AppendEntriesResponse(const AppendEntriesRequest&)>;

    virtual void SetRequestVoteHandler(RequestVoteHandler handler) = 0;
    virtual void SetAppendEntriesHandler(AppendEntriesHandler handler) = 0;

    // 启动/停止
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

// 简单的内存传输实现 (用于测试和单机模式)
class InMemoryTransport : public Transport {
public:
    void SendRequestVote(NodeId, const RequestVoteRequest&) override {}
    void SendAppendEntries(NodeId, const AppendEntriesRequest&) override {}
    void SetRequestVoteHandler(RequestVoteHandler) override {}
    void SetAppendEntriesHandler(AppendEntriesHandler) override {}
    void Start() override {}
    void Stop() override {}
};

} // namespace myetcd