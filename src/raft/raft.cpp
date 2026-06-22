#include "raft/raft_node.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <sstream>

namespace myetcd {

RaftNode::RaftNode(const ClusterConfig& config)
    : config_(config)
    , node_id_(config.node_id)
    , rng_(std::chrono::steady_clock::now().time_since_epoch().count())
{
    for (const auto& peer : config.initial_cluster) {
        // 解析 "name=addr" 格式，简化处理
        NodeId id = peers_.size() + 1;
        if (id != node_id_) {
            peers_.insert(id);
        }
    }
}

RaftNode::~RaftNode() {
    Stop();
}

void RaftNode::Start() {
    running_ = true;
    worker_thread_ = std::thread(&RaftNode::Run, this);
}

void RaftNode::Stop() {
    running_ = false;
    cv_.notify_all();
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void RaftNode::Run() {
    BecomeFollower(0);
    
    while (running_) {
        switch (state_.load()) {
        case NodeState::Follower: {
            auto timeout = RandomElectionTimeout();
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait_for(lock, std::chrono::milliseconds(timeout), [this] {
                return !running_;
            });
            if (!running_) break;
            // 选举超时，转为 Candidate
            BecomeCandidate();
            break;
        }
        case NodeState::Candidate: {
            StartElection();
            auto timeout = RandomElectionTimeout();
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait_for(lock, std::chrono::milliseconds(timeout), [this] {
                return !running_ || state_.load() != NodeState::Candidate;
            });
            break;
        }
        case NodeState::Leader: {
            BroadcastHeartbeat();
            AdvanceCommitIndex();
            ApplyCommittedEntries();
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait_for(lock, std::chrono::milliseconds(config_.heartbeat_interval_ms), [this] {
                return !running_ || state_.load() != NodeState::Leader;
            });
            break;
        }
        }
    }
}

void RaftNode::BecomeFollower(Term term) {
    if (term > current_term_.load()) {
        current_term_ = term;
        voted_for_ = kNoNodeId;
    }
    state_ = NodeState::Follower;
    if (state_change_handler_) {
        state_change_handler_(NodeState::Follower);
    }
}

void RaftNode::BecomeCandidate() {
    state_ = NodeState::Candidate;
    current_term_ = current_term_.load() + 1;
    voted_for_ = node_id_;
    leader_id_ = kNoNodeId;
    if (state_change_handler_) {
        state_change_handler_(NodeState::Candidate);
    }
}

void RaftNode::BecomeLeader() {
    state_ = NodeState::Leader;
    leader_id_ = node_id_;
    
    // 初始化 next_index 和 match_index
    Index last_idx = log_.LastIndex();
    next_index_.clear();
    match_index_.clear();
    for (auto peer : peers_) {
        next_index_[peer] = last_idx + 1;
        match_index_[peer] = 0;
    }
    
    if (state_change_handler_) {
        state_change_handler_(NodeState::Leader);
    }
}

void RaftNode::StartElection() {
    Term term = current_term_.load();
    Index last_log_index = log_.LastIndex();
    Term last_log_term = log_.LastTerm();
    
    RequestVoteRequest req;
    req.term = term;
    req.candidate_id = node_id_;
    req.last_log_index = last_log_index;
    req.last_log_term = last_log_term;
    
    // 在单节点或测试模式下，直接成为 Leader
    if (peers_.empty()) {
        BecomeLeader();
        return;
    }
    
    // 投票计数
    int votes = 1; // 自己投自己
    int needed = static_cast<int>(peers_.size() + 1) / 2 + 1;
    
    for (auto peer : peers_) {
        (void)peer;
        if (transport_) {
            // 简化处理：在单节点模式下直接成为 Leader
            // 多节点模式下需要通过 transport 发送 RPC
        }
    }
    
    // 简化：如果获得多数票则成为 Leader
    if (votes >= needed) {
        BecomeLeader();
    }
}

void RaftNode::BroadcastHeartbeat() {
    AppendEntriesRequest req;
    req.term = current_term_.load();
    req.leader_id = node_id_;
    req.leader_commit = commit_index_.load();
    
    for (auto peer : peers_) {
        req.prev_log_index = next_index_[peer] - 1;
        req.prev_log_term = log_.TermAt(req.prev_log_index);
        
        // 获取需要发送的日志
        auto entries = log_.EntriesFrom(next_index_[peer]);
        req.entries = entries;
        
        if (transport_) {
            transport_->SendAppendEntries(peer, req);
        }
    }
}

void RaftNode::AdvanceCommitIndex() {
    if (state_ != NodeState::Leader) return;
    
    std::vector<Index> match_indices;
    match_indices.push_back(log_.LastIndex()); // 自己的
    
    for (auto& [peer, idx] : match_index_) {
        match_indices.push_back(idx);
    }
    
    std::sort(match_indices.begin(), match_indices.end(), std::greater<Index>());
    
    int majority = static_cast<int>(peers_.size() + 1) / 2 + 1;
    if (majority <= static_cast<int>(match_indices.size())) {
        Index new_commit = match_indices[majority - 1];
        if (new_commit > commit_index_.load()) {
            if (log_.TermAt(new_commit) == current_term_.load()) {
                commit_index_ = new_commit;
            }
        }
    }
}

void RaftNode::ApplyCommittedEntries() {
    Index committed = commit_index_.load();
    Index applied = last_applied_.load();
    
    if (committed > applied) {
        auto entries = log_.Slice(applied + 1, committed + 1);
        
        {
            std::lock_guard<std::mutex> lock(pending_mu_);
            pending_committed_.insert(pending_committed_.end(), entries.begin(), entries.end());
        }
        
        last_applied_ = committed;
        
        if (ready_handler_ && !entries.empty()) {
            ready_handler_(entries);
        }
    }
}

ProposalResult RaftNode::Propose(const RaftEntry& entry) {
    if (!IsLeader()) {
        ProposalResult result;
        result.accepted = false;
        std::ostringstream oss;
        oss << "not leader, current leader is " << leader_id_.load();
        result.error = oss.str();
        return result;
    }
    
    RaftEntry e = entry;
    e.term = current_term_.load();
    e.index = log_.LastIndex() + 1;
    
    log_.Append(e);
    
    // 更新自己的 match_index
    match_index_[node_id_] = e.index;
    
    ProposalResult result;
    result.index = e.index;
    result.accepted = true;
    return result;
}

RequestVoteResponse RaftNode::HandleRequestVote(const RequestVoteRequest& req) {
    RequestVoteResponse resp;
    resp.term = current_term_.load();
    resp.vote_granted = false;
    
    if (req.term < current_term_.load()) {
        return resp;
    }
    
    if (req.term > current_term_.load()) {
        BecomeFollower(req.term);
        resp.term = req.term;
    }
    
    // 检查是否已投票
    if (voted_for_.load() == kNoNodeId || voted_for_.load() == req.candidate_id) {
        // 检查日志是否至少和当前一样新
        Term last_log_term = log_.LastTerm();
        Index last_log_index = log_.LastIndex();
        
        bool log_ok = (req.last_log_term > last_log_term) ||
                      (req.last_log_term == last_log_term && req.last_log_index >= last_log_index);
        
        if (log_ok) {
            voted_for_ = req.candidate_id;
            resp.vote_granted = true;
        }
    }
    
    return resp;
}

AppendEntriesResponse RaftNode::HandleAppendEntries(const AppendEntriesRequest& req) {
    AppendEntriesResponse resp;
    resp.term = current_term_.load();
    resp.success = false;
    resp.last_log_index = log_.LastIndex();
    
    if (req.term < current_term_.load()) {
        return resp;
    }
    
    // 收到有效的 AppendEntries，重置选举计时器
    BecomeFollower(req.term);
    leader_id_ = req.leader_id;
    cv_.notify_all();
    
    resp.term = req.term;
    
    // 检查 prev_log_index 是否匹配
    if (req.prev_log_index > 0) {
        Term local_term = log_.TermAt(req.prev_log_index);
        if (local_term != req.prev_log_term) {
            resp.success = false;
            return resp;
        }
    }
    
    // 处理冲突：删除冲突的日志
    if (!req.entries.empty()) {
        Index first_new_idx = req.entries[0].index;
        log_.TruncateTo(first_new_idx - 1);
        
        for (const auto& entry : req.entries) {
            log_.Append(entry);
        }
    }
    
    // 更新 commit_index
    if (req.leader_commit > commit_index_.load()) {
        commit_index_ = std::min(req.leader_commit, log_.LastIndex());
        ApplyCommittedEntries();
    }
    
    resp.success = true;
    resp.last_log_index = log_.LastIndex();
    return resp;
}

void RaftNode::ApplySnapshot(const Snapshot& snapshot) {
    log_.Restore(snapshot.last_index);
    commit_index_ = snapshot.last_index;
    last_applied_ = snapshot.last_index;
    if (snapshot.last_term > current_term_.load()) {
        current_term_ = snapshot.last_term;
    }
}

Snapshot RaftNode::CreateSnapshot() const {
    Snapshot snap;
    snap.last_index = log_.LastIndex();
    snap.last_term = log_.LastTerm();
    return snap;
}

std::vector<RaftEntry> RaftNode::GetCommittedEntries() {
    std::lock_guard<std::mutex> lock(pending_mu_);
    auto entries = std::move(pending_committed_);
    pending_committed_.clear();
    return entries;
}

int64_t RaftNode::RandomElectionTimeout() {
    std::uniform_int_distribution<int64_t> dist(
        config_.election_timeout_ms,
        config_.max_election_timeout_ms
    );
    return dist(rng_);
}

} // namespace myetcd