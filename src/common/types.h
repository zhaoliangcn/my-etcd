#pragma once

// Workaround: some system/transitive headers may define `entry` as a macro,
// which corrupts identifiers like `RaftEntry` during preprocessing.
#ifdef entry
#undef entry
#endif

#include <cstdint>
#include <string>
#include <vector>
#include <map>
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
    CONF_CHANGE = 2,  // 集群成员变更
};

// 成员变更类型
enum class ConfChangeType : uint8_t {
    AddNode = 0,
    RemoveNode = 1,
};

// 成员变更条目
struct ConfChange {
    ConfChangeType type = ConfChangeType::AddNode;
    NodeId node_id = kNoNodeId;
    std::string peer_addr;  // "host:port"
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
    ConfChange conf_change;  // 成员变更数据（type == CONF_CHANGE 时有效）
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
    EventType type = EventType::PUT;
    KeyValue kv;
    KeyValue prev_kv;
};

// 事务类型

// 比较操作符
enum class CompareOp : uint8_t {
    Equal            = 0,
    NotEqual         = 1,
    Greater          = 2,
    Less             = 3,
    GreaterOrEqual   = 4,
    LessOrEqual      = 5,
};

// 比较目标字段
enum class CompareTarget : uint8_t {
    Value        = 0,  // 比较 key 的值
    ModRevision  = 1,  // 比较修改版本号
    CreateRevision = 2,// 比较创建版本号
    Version      = 3,  // 比较版本号
};

// 事务比较条件
struct TxnCompare {
    CompareTarget target = CompareTarget::Value;
    CompareOp op = CompareOp::Equal;
    std::string key;
    std::string value;           // 用于 Value 比较
    int64_t revision = 0;        // 用于 Revision/Version 比较
};

// 事务操作结果
struct TxnOpResult {
    bool success = false;
    std::optional<KeyValue> prev_kv;
};

// KV 事务的单个操作
struct TxnOp {
    EventType type = EventType::PUT;  // PUT or DELETE
    std::string key;
    std::string value;
    LeaseId lease_id = 0;
};

// 事务请求体（JSON 格式 POST）
struct TxnRequest {
    std::vector<TxnCompare> compare;     // 条件列表（AND）
    std::vector<TxnOp> success;          // 条件满足时执行
    std::vector<TxnOp> failure;          // 条件不满足时执行
};

// 事务响应
struct TxnResponse {
    bool succeeded = false;
    std::vector<TxnOpResult> results;
    Index raft_index = 0;
};

// 集群配置
struct ClusterConfig {
    NodeId node_id = 1;
    std::string name = "default";
    std::string data_dir = "./data";
    std::string listen_addr = "0.0.0.0:2379";
    std::string listen_peer_addr = "0.0.0.0:2380";
    std::vector<std::string> initial_cluster;
    std::map<NodeId, std::string> peer_addresses;      // node_id → "host:port" 用于 TCP 传输
    
    // 时间参数 (毫秒)
    int64_t election_timeout_ms = 1000;
    int64_t heartbeat_interval_ms = 100;
    int64_t max_election_timeout_ms = 2000;
    int64_t snapshot_interval = 10000;
    int64_t max_snapshots = 5;
    int64_t max_wal_files = 5;
};

} // namespace myetcd