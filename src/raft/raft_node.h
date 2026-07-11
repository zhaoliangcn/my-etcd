#pragma once

#include "common/types.h"
#include "raft/raft_log.h"
#include "raft/transport.h"
#include <random>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <functional>
#include <map>
#include <set>

namespace myetcd {

// Raft 提案结果
struct ProposalResult {
    Index index;
    bool accepted;
    std::string error;
};

// Raft 节点 - 核心共识算法实现
class RaftNode {
public:
    using ReadyHandler = std::function<void(const std::vector<RaftEntry>&)>;
    using StateChangeHandler = std::function<void(NodeState)>;
    using SnapshotHandler = std::function<void(Index)>;
    using ConfChangeHandler = std::function<void(const ConfChange&)>;

    explicit RaftNode(const ClusterConfig& config);
    ~RaftNode();

    // 禁止拷贝
    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    // 启动 Raft 节点
    void Start();

    // 停止 Raft 节点
    void Stop();

    // 提案一个操作
    ProposalResult Propose(const RaftEntry& entry);

    // 批量提案 - 缓存并合并写入日志
    ProposalResult ProposeBatch(const RaftEntry& entry);

    // 刷新批量缓冲区
    void FlushBatch();

    // 处理 RequestVote
    RequestVoteResponse HandleRequestVote(const RequestVoteRequest& req);

    // 处理 AppendEntries
    AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& req);

    // 获取当前状态
    NodeState State() const { return state_.load(); }
    Term CurrentTerm() const { return current_term_.load(); }
    NodeId LeaderId() const { return leader_id_.load(); }
    bool IsLeader() const { return state_.load() == NodeState::Leader; }

    // 设置回调
    void SetReadyHandler(ReadyHandler handler) { ready_handler_ = std::move(handler); }
    void SetStateChangeHandler(StateChangeHandler handler) { state_change_handler_ = std::move(handler); }
    void SetSnapshotHandler(SnapshotHandler handler) { snapshot_handler_ = std::move(handler); }
    void SetConfChangeHandler(ConfChangeHandler handler) { conf_change_handler_ = std::move(handler); }

    // 设置传输层
    void SetTransport(std::shared_ptr<Transport> transport) { transport_ = std::move(transport); }

    // 添加/移除节点（直接操作，不经过 Raft 协议）
    void AddNode(NodeId id);
    void RemoveNode(NodeId id);

    // 通过 Raft 协议提案成员变更（仅 Leader 可用）
    ProposalResult ProposeConfChange(const ConfChange& cc);

    // 应用成员变更（回调 conf_change_handler_ 后执行内部状态更新）
    void ApplyConfChange(const ConfChange& cc);

    // 获取当前 peers
    std::set<NodeId> GetPeers() const { return peers_; }
    bool HasPeer(NodeId id) const { return peers_.count(id) > 0; }

    // 快照相关
    void ApplySnapshot(const Snapshot& snapshot);
    Snapshot CreateSnapshot() const;

    // 获取已提交的日志
    std::vector<RaftEntry> GetCommittedEntries();

    // 获取 RaftLog
    RaftLog& GetLog() { return log_; }

private:
    void Run();
    void BecomeFollower(Term term);
    void BecomeCandidate();
    void BecomeLeader();
    void StartElection();
    void BroadcastHeartbeat();
    void AdvanceCommitIndex();
    void ApplyCommittedEntries();

    // 内部版本：调用者必须已持有 mu_
    void BecomeFollowerLocked(Term term);
    void BecomeCandidateLocked();
    void BecomeLeaderLocked();

    // 生成随机选举超时
    int64_t RandomElectionTimeout();

    // 二分查找冲突 term 对应的第一条日志索引
    Index FindConflictTermIndex(Term conflict_term, Index conflict_index_hint);

    ClusterConfig config_;
    NodeId node_id_;

    // 持久化状态
    std::atomic<Term> current_term_{0};
    std::atomic<NodeId> voted_for_{kNoNodeId};
    RaftLog log_;

    // 易失性状态
    std::atomic<Index> commit_index_{0};
    std::atomic<Index> last_applied_{0};
    std::atomic<NodeState> state_{NodeState::Follower};
    std::atomic<NodeId> leader_id_{kNoNodeId};

    // Leader 状态
    std::map<NodeId, Index> next_index_;
    std::map<NodeId, Index> match_index_;

    // 其他节点
    std::set<NodeId> peers_;

    // 传输层
    std::shared_ptr<Transport> transport_;

    // 回调
    ReadyHandler ready_handler_;
    StateChangeHandler state_change_handler_;
    SnapshotHandler snapshot_handler_;
    ConfChangeHandler conf_change_handler_;

    // 线程
    std::thread worker_thread_;
    std::atomic<bool> running_{false};
    std::mutex mu_;
    std::condition_variable cv_;

    // 随机数
    std::mt19937 rng_;

    // 已提交但未应用的条目
    std::vector<RaftEntry> pending_committed_;
    std::mutex pending_mu_;

    // 批量提交缓冲区
    std::vector<RaftEntry> batch_buffer_;
    std::mutex batch_mu_;
    static constexpr size_t kMaxBatchSize = 64;
};

} // namespace myetcd