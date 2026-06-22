#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include <chrono>
#include <functional>

namespace myetcd {

using Revision = int64_t;
using Term = uint64_t;
using Index = uint64_t;
using NodeId = uint64_t;
using LeaseId = int64_t;

constexpr Revision kNoRevision = 0;
constexpr Term kNoTerm = 0;
constexpr Index kNoIndex = 0;
constexpr NodeId kNoNodeId = 0;

// KV 操作类型
enum class EventType : int {
    PUT = 0,
    DELETE = 1,
};

// Raft 节点状态
enum class NodeState : int {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
};

// Raft 日志条目
struct RaftEntry {
    Term term = kNoTerm;
    Index index = kNoIndex;
    EventType type = EventType::PUT;
    std::string key;
    std::string value;
    LeaseId lease_id = 0;
};

// Raft 快照
struct Snapshot {
    Index last_index = kNoIndex;
    Term last_term = kNoTerm;
    std::vector<uint8_t> data;
};

// Raft 硬状态 (持久化)
struct RaftHardState {
    Term current_term = kNoTerm;
    std::optional<NodeId> voted_for;
    Index commit_index = kNoIndex;
};

// KV 键值对
struct KeyValue {
    std::string key;
    std::string value;
    Revision create_revision = kNoRevision;
    Revision mod_revision = kNoRevision;
    int64_t version = 0;
    LeaseId lease_id = 0;
};

// Watch 事件
struct WatchEvent {
    EventType type;
    KeyValue kv;
    KeyValue prev_kv;
};

// 集群配置
struct ClusterConfig {
    NodeId node_id = 1;
    std::string name = "default";
    std::string data_dir = "./data";
    std::string listen_addr = "0.0.0.0:2379";
    std::string listen_peer_addr = "0.0.0.0:2380";
    std::vector<std::string> initial_cluster;
    
    // 时间参数 (毫秒)
    int64_t election_timeout_ms = 1000;
    int64_t heartbeat_interval_ms = 100;
    int64_t max_election_timeout_ms = 2000;
    int64_t snapshot_interval = 10000;
    int64_t max_snapshots = 5;
    int64_t max_wal_files = 5;
};

} // namespace myetcd