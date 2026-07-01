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

    // 发送 RequestVote RPC（同步，返回响应）
    virtual RequestVoteResponse SendRequestVote(NodeId target, const RequestVoteRequest& req) = 0;

    // 发送 AppendEntries RPC（同步，返回响应）
    virtual AppendEntriesResponse SendAppendEntries(NodeId target, const AppendEntriesRequest& req) = 0;

    // 设置回调
    using RequestVoteHandler = std::function<RequestVoteResponse(const RequestVoteRequest&)>;
    using AppendEntriesHandler = std::function<AppendEntriesResponse(const AppendEntriesRequest&)>;

    virtual void SetRequestVoteHandler(RequestVoteHandler handler) = 0;
    virtual void SetAppendEntriesHandler(AppendEntriesHandler handler) = 0;

    // 启动/停止
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

// 简单的内存传输实现（用于测试和单机模式）
class InMemoryTransport : public Transport {
public:
    RequestVoteResponse SendRequestVote(NodeId target, const RequestVoteRequest& req) override {
        (void)target;
        // 直接调用本地的 handler（进程内路由）
        if (request_vote_handler_) {
            return request_vote_handler_(req);
        }
        RequestVoteResponse resp;
        resp.term = 0;
        resp.vote_granted = false;
        return resp;
    }

    AppendEntriesResponse SendAppendEntries(NodeId target, const AppendEntriesRequest& req) override {
        (void)target;
        // 直接调用本地的 handler（进程内路由）
        if (append_entries_handler_) {
            return append_entries_handler_(req);
        }
        AppendEntriesResponse resp;
        resp.term = 0;
        resp.success = false;
        resp.last_log_index = 0;
        return resp;
    }

    void SetRequestVoteHandler(RequestVoteHandler handler) override {
        request_vote_handler_ = std::move(handler);
    }

    void SetAppendEntriesHandler(AppendEntriesHandler handler) override {
        append_entries_handler_ = std::move(handler);
    }

    void Start() override {}
    void Stop() override {}

private:
    RequestVoteHandler request_vote_handler_;
    AppendEntriesHandler append_entries_handler_;
};

} // namespace myetcd