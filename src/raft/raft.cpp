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
        (void)peer;
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

    // 在单节点模式下，直接成为 Leader
    if (peers_.empty()) {
        BecomeLeader();
        return;
    }

    // 投票计数：自己投自己
    int votes = 1;
    int needed = static_cast<int>(peers_.size() + 1) / 2 + 1;

    for (auto peer : peers_) {
        if (!transport_) continue;

        auto resp = transport_->SendRequestVote(peer, req);

        // 如果响应的任期更大，转为 Follower
        if (resp.term > current_term_.load()) {
            BecomeFollower(resp.term);
            return;
        }

        if (resp.vote_granted) {
            votes++;
        }
    }

    if (votes >= needed) {
        BecomeLeader();
    }
}

void RaftNode::BroadcastHeartbeat() {
    // Leader 先刷新批量缓冲区
    FlushBatch();

    auto current_commit = commit_index_.load();

    for (auto peer : peers_) {
        Index prev_log_index = next_index_[peer] - 1;
        Term prev_log_term = log_.TermAt(prev_log_index);

        auto entries = log_.EntriesFrom(next_index_[peer]);

        // 限制单次发送条目数（流水线控制）
        const size_t max_entries_per_msg = 128;
        if (entries.size() > max_entries_per_msg) {
            entries.resize(max_entries_per_msg);
        }

        AppendEntriesRequest req;
        req.term = current_term_.load();
        req.leader_id = node_id_;
        req.prev_log_index = prev_log_index;
        req.prev_log_term = prev_log_term;
        req.entries = entries;
        req.leader_commit = current_commit;

        if (!transport_) continue;

        auto resp = transport_->SendAppendEntries(peer, req);

        // 任期更大则转为 Follower
        if (resp.term > current_term_.load()) {
            BecomeFollower(resp.term);
            return;
        }

        if (resp.success) {
            // 更新 match_index 和 next_index
            if (!entries.empty()) {
                match_index_[peer] = entries.back().index;
                next_index_[peer] = match_index_[peer] + 1;
            } else {
                // 心跳：至少更新到 follower 报告的 last_log_index
                match_index_[peer] = std::max(match_index_[peer], resp.last_log_index);
            }
        } else {
            // ----- 优化：二分查找回退 -----
            if (resp.conflict_term > 0) {
                // 找到 Leader 日志中冲突 term 的第一条索引
                Index new_next = FindConflictTermIndex(resp.conflict_term, resp.conflict_index);
                if (new_next > 0 && new_next < next_index_[peer]) {
                    next_index_[peer] = new_next;
                } else {
                    // 如果没找到，回退到冲突索引
                    if (resp.conflict_index > 0 && resp.conflict_index < next_index_[peer]) {
                        next_index_[peer] = resp.conflict_index;
                    } else if (next_index_[peer] > 1) {
                        next_index_[peer]--;
                    }
                }
            } else if (resp.conflict_index > 0) {
                // Follower 没有冲突 term 信息，使用 conflict_index
                if (resp.conflict_index < next_index_[peer]) {
                    next_index_[peer] = resp.conflict_index;
                } else if (next_index_[peer] > 1) {
                    next_index_[peer]--;
                }
            } else {
                // 回退
                if (next_index_[peer] > 1) {
                    next_index_[peer]--;
                }
            }
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

ProposalResult RaftNode::ProposeBatch(const RaftEntry& entry) {
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

    // 先放入缓冲区，攒批后在 BroadcastHeartbeat 中一并写入日志
    ProposalResult result;
    {
        std::lock_guard<std::mutex> lock(batch_mu_);
        e.index = log_.LastIndex() + 1 + batch_buffer_.size();
        batch_buffer_.push_back(e);
        result.index = e.index;
    }
    result.accepted = true;
    return result;
}

void RaftNode::FlushBatch() {
    std::vector<RaftEntry> batch;
    {
        std::lock_guard<std::mutex> lock(batch_mu_);
        if (batch_buffer_.empty()) return;
        batch.swap(batch_buffer_);
    }

    if (batch.empty()) return;

    // 修正索引（ProposeBatch 时用的是预测索引，此时需要确保精确）
    Index base = log_.LastIndex() + 1;
    for (size_t i = 0; i < batch.size(); ++i) {
        batch[i].index = base + static_cast<Index>(i);
    }

    log_.Append(batch);
    match_index_[node_id_] = batch.back().index;
}

Index RaftNode::FindConflictTermIndex(Term conflict_term, Index conflict_index_hint) {
    Index first = log_.FirstIndex();
    Index last = log_.LastIndex();

    // 如果 conflict_term 在当前日志中不存在（说明 Leader 中没有该 term 的条目），
    // 直接回退到 conflict_index_hint
    bool has_term = false;
    for (Index i = first; i <= last; ++i) {
        if (log_.TermAt(i) == conflict_term) {
            has_term = true;
            break;
        }
    }
    if (!has_term) return (conflict_index_hint > 0) ? conflict_index_hint : 1;

    // 二分查找 conflict_term 的第一条日志索引
    Index lo = first;
    Index hi = last + 1;
    while (lo < hi) {
        Index mid = lo + (hi - lo) / 2;
        Term t = log_.TermAt(mid);
        if (t < conflict_term) {
            lo = mid + 1;
        } else if (t > conflict_term) {
            hi = mid;
        } else {
            // 找到了 conflict_term，继续向左找第一条
            hi = mid;
        }
    }

    Index result = lo;
    // 确保是有效的索引
    if (result >= first && result <= last && log_.TermAt(result) == conflict_term) {
        return result;
    }

    // 回退方案
    return (conflict_index_hint > 0) ? conflict_index_hint : (first > 0 ? first : 1);
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

            // ---- 添加冲突信息用于二分查找回退 ----
            // 获取冲突索引处的 term
            Term conflict_term = log_.TermAt(req.prev_log_index);
            if (conflict_term != kNoTerm) {
                resp.conflict_term = conflict_term;
            }
            // 找到该 term 的第一条日志索引
            Index first_idx = log_.FirstIndex();
            for (Index i = req.prev_log_index; i >= first_idx && i > 0; --i) {
                if (log_.TermAt(i) != conflict_term) {
                    resp.conflict_index = i + 1;
                    break;
                }
            }
            if (resp.conflict_index == 0) {
                resp.conflict_index = first_idx;
            }

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