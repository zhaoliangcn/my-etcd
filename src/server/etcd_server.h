#pragma once

#include "common/types.h"
#include "raft/raft.h"
#include "raft/tcp_transport.h"
#include "wal/wal.h"
#include "snapshot/snapshot.h"
#include "storage/kvstore.h"
#include "lease/lease.h"
#include "watch/watcher.h"
#include "server/http_handler.h"
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>

namespace myetcd {

// EtcdServer - 主服务端，整合所有组件
class EtcdServer {
public:
    explicit EtcdServer(const ClusterConfig& config);
    ~EtcdServer();

    EtcdServer(const EtcdServer&) = delete;
    EtcdServer& operator=(const EtcdServer&) = delete;

    // 启动服务
    bool Start();

    // 停止服务
    void Stop();

    // 是否运行中
    bool IsRunning() const { return running_.load(); }

    // KV 操作
    HttpResponse Put(const std::string& key, const std::string& value, LeaseId lease_id = 0);
    HttpResponse Delete(const std::string& key);
    HttpResponse Range(const std::string& key, const std::string& range_end = "",
                       Revision revision = 0, int64_t limit = 0);
    HttpResponse Txn(const std::string& request_body);

    // Watch 操作
    HttpResponse Watch(const std::string& key, Revision start_rev = 0, bool prefix = false);
    HttpResponse WatchCancel(LeaseId watch_id);

    // Lease 操作
    HttpResponse LeaseGrant(int64_t ttl_seconds);
    HttpResponse LeaseRevoke(LeaseId id);
    HttpResponse LeaseRenew(LeaseId id);
    HttpResponse LeaseKeepAlive(LeaseId id);

    // 集群信息
    HttpResponse ClusterInfo();

    // 成员变更
    HttpResponse MemberAdd(const std::string& peer_addr);
    HttpResponse MemberRemove(NodeId id);
    void OnConfChangeApplied(const ConfChange& cc);

    // 获取 KVStore
    KVStore& GetKVStore() { return *kvstore_; }

    // 获取 RaftNode
    RaftNode& GetRaftNode() { return *raft_node_; }

private:
    void RaftReadyHandler(const std::vector<RaftEntry>& entries);
    void ProcessRaftEntries(const std::vector<RaftEntry>& entries);
    void SnapshotLoop();
    void OnLeaseExpired(const std::string& key);

    ClusterConfig config_;
    std::unique_ptr<WAL> wal_;
    std::unique_ptr<SnapshotManager> snapshot_mgr_;
    std::unique_ptr<KVStore> kvstore_;
    std::unique_ptr<RaftNode> raft_node_;
    std::unique_ptr<LeaseManager> lease_mgr_;
    std::unique_ptr<WatchManager> watch_mgr_;
    std::shared_ptr<TcpTransport> transport_;

    std::atomic<bool> running_{false};
    std::thread snapshot_thread_;
    std::mutex server_mu_;
    Index snapshot_index_ = 0;  // 上次快照的索引
};

} // namespace myetcd