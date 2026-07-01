#include "server/etcd_server.h"
#include <iostream>
#include <chrono>
#include <sstream>

namespace myetcd {

EtcdServer::EtcdServer(const ClusterConfig& config)
    : config_(config)
{
    wal_ = std::make_unique<WAL>(config.data_dir);
    snapshot_mgr_ = std::make_unique<SnapshotManager>(
        config.data_dir + "/snap", config.max_snapshots);
    kvstore_ = std::make_unique<KVStore>(config.data_dir);
    raft_node_ = std::make_unique<RaftNode>(config);
    lease_mgr_ = std::make_unique<LeaseManager>();
    watch_mgr_ = std::make_unique<WatchManager>();
}

EtcdServer::~EtcdServer() {
    Stop();
}

bool EtcdServer::Start() {
    std::cout << "[Server] Starting my-etcd server..." << std::endl;
    std::cout << "[Server] Node ID: " << config_.node_id << std::endl;
    std::cout << "[Server] Data directory: " << config_.data_dir << std::endl;

    // 1. 打开 WAL
    if (!wal_->Open()) {
        std::cerr << "[Server] Failed to open WAL" << std::endl;
        return false;
    }

    // 2. 恢复 Raft 硬状态
    RaftHardState hard_state;
    if (wal_->LoadHardState(hard_state)) {
        std::cout << "[Server] Restored hard state: term=" << hard_state.current_term
                  << " commit=" << hard_state.commit_index << std::endl;
    }

    // 3. 加载快照
    Snapshot snap;
    if (snapshot_mgr_->LoadLatestSnapshot(snap)) {
        std::cout << "[Server] Restored snapshot: last_index=" << snap.last_index
                  << " last_term=" << snap.last_term << std::endl;
        raft_node_->ApplySnapshot(snap);
        if (!snap.data.empty()) {
            kvstore_->Deserialize(snap.data);
        }
    }

    // 4. 重放 WAL 日志
    auto wal_entries = wal_->ReadAllEntries();
    std::cout << "[Server] Replaying " << wal_entries.size() << " WAL entries" << std::endl;
    kvstore_->RestoreFromEntries(wal_entries);

    // 5. 打开 KVStore
    if (!kvstore_->Open()) {
        std::cerr << "[Server] Failed to open KVStore" << std::endl;
        return false;
    }

    // 6. 设置 Raft 回调
    raft_node_->SetReadyHandler(
        std::bind(&EtcdServer::RaftReadyHandler, this, std::placeholders::_1));
    raft_node_->SetSnapshotHandler([this](Index idx) {
        std::cout << "[Server] Creating snapshot at index " << idx << std::endl;
        auto data = kvstore_->Serialize();
        snapshot_mgr_->CreateSnapshot(idx, raft_node_->CurrentTerm(), data);
        wal_->TruncateFrom(idx);
    });

    // 7. 设置租约过期回调
    lease_mgr_->SetExpireCallback(
        std::bind(&EtcdServer::OnLeaseExpired, this, std::placeholders::_1));

    // 8. 启动各组件
    lease_mgr_->Start();
    raft_node_->Start();

    // 9. 启动快照循环
    running_ = true;
    snapshot_thread_ = std::thread(&EtcdServer::SnapshotLoop, this);

    std::cout << "[Server] my-etcd server started successfully!" << std::endl;
    std::cout << "[Server] Raft state: " 
              << (raft_node_->IsLeader() ? "Leader" : "Follower") << std::endl;

    return true;
}

void EtcdServer::Stop() {
    running_ = false;
    if (snapshot_thread_.joinable()) {
        snapshot_thread_.join();
    }
    if (raft_node_) raft_node_->Stop();
    if (lease_mgr_) lease_mgr_->Stop();
    if (wal_) wal_->Close();
    if (kvstore_) kvstore_->Close();
    std::cout << "[Server] my-etcd server stopped" << std::endl;
}

void EtcdServer::RaftReadyHandler(const std::vector<RaftEntry>& entries) {
    ProcessRaftEntries(entries);

    // 持久化到 WAL
    wal_->AppendEntries(entries);

    // 保存硬状态
    RaftHardState state;
    state.current_term = raft_node_->CurrentTerm();
    state.commit_index = raft_node_->GetLog().LastIndex();
    wal_->SaveHardState(state);
}

void EtcdServer::ProcessRaftEntries(const std::vector<RaftEntry>& entries) {
    for (const auto& entry : entries) {
        if (entry.type == EventType::PUT) {
            // 原子操作：一次加锁完成读旧值 + 写入新值
            auto result = kvstore_->PutWithPrev(entry.key, entry.value, entry.lease_id);

            // 关联租约
            if (entry.lease_id > 0) {
                lease_mgr_->Attach(entry.lease_id, entry.key);
            }

            // 通知 Watch
            if (watch_mgr_ && result.new_kv) {
                WatchEvent ev;
                ev.type = EventType::PUT;
                ev.kv = *result.new_kv;
                if (result.prev_kv) {
                    ev.prev_kv = *result.prev_kv;
                } else {
                    ev.prev_kv.key = entry.key;
                }
                watch_mgr_->Notify(ev);
            }
        } else if (entry.type == EventType::DELETE) {
            KeyValue prev_kv;
            if (watch_mgr_) {
                auto existing = kvstore_->Get(entry.key);
                if (existing) {
                    prev_kv = *existing;
                } else {
                    prev_kv.key = entry.key;
                }
            }

            kvstore_->Delete(entry.key);

            // 通知 Watch
            if (watch_mgr_) {
                WatchEvent ev;
                ev.type = EventType::DELETE;
                ev.kv.key = entry.key;
                ev.prev_kv = prev_kv;
                watch_mgr_->Notify(ev);
            }
        }
    }
}

void EtcdServer::SnapshotLoop() {
    while (running_) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(config_.snapshot_interval));

        if (!running_) break;

        Index last_applied = raft_node_->GetLog().LastIndex();
        Index snapshot_index = 0; // TODO: 跟踪上次快照索引

        if (snapshot_mgr_->ShouldSnapshot(last_applied, snapshot_index)) {
            std::lock_guard<std::mutex> lock(server_mu_);
            std::cout << "[Server] Taking snapshot at index " << last_applied << std::endl;
            auto data = kvstore_->Serialize();
            snapshot_mgr_->CreateSnapshot(last_applied, raft_node_->CurrentTerm(), data);
            wal_->TruncateFrom(last_applied);
        }
    }
}

void EtcdServer::OnLeaseExpired(const std::string& key) {
    std::cout << "[Server] Lease expired for key: " << key << std::endl;
    // 通过 Raft 删除过期的 key
    if (raft_node_->IsLeader()) {
        RaftEntry entry;
        entry.type = EventType::DELETE;
        entry.key = key;
        raft_node_->Propose(entry);
    }
}

// KV 操作

HttpResponse EtcdServer::Put(const std::string& key, const std::string& value, LeaseId lease_id) {
    HttpResponse resp;

    if (!raft_node_->IsLeader()) {
        resp.SetError(503, "not leader, current leader is " + std::to_string(raft_node_->LeaderId()));
        return resp;
    }

    RaftEntry entry;
    entry.type = EventType::PUT;
    entry.key = key;
    entry.value = value;
    entry.lease_id = lease_id;

    auto result = raft_node_->Propose(entry);
    if (!result.accepted) {
        resp.SetError(500, result.error);
        return resp;
    }

    // 等待提交 (简化实现：直接返回)
    std::ostringstream oss;
    oss << "{\"header\":{\"revision\":" << result.index << "},\"succeeded\":true}";
    resp.SetJson(oss.str());
    return resp;
}

HttpResponse EtcdServer::Delete(const std::string& key) {
    HttpResponse resp;

    if (!raft_node_->IsLeader()) {
        resp.SetError(503, "not leader");
        return resp;
    }

    RaftEntry entry;
    entry.type = EventType::DELETE;
    entry.key = key;

    auto result = raft_node_->Propose(entry);
    if (!result.accepted) {
        resp.SetError(500, result.error);
        return resp;
    }

    std::ostringstream oss;
    oss << "{\"header\":{\"revision\":" << result.index << "},\"deleted\":1}";
    resp.SetJson(oss.str());
    return resp;
}

HttpResponse EtcdServer::Range(const std::string& key, const std::string& range_end,
                                 Revision revision, int64_t limit) {
    HttpResponse resp;

    std::vector<KeyValue> kvs;
    if (range_end.empty()) {
        auto kv = kvstore_->Get(key, revision);
        if (kv) {
            kvs.push_back(*kv);
        }
    } else {
        kvs = kvstore_->Range(key, range_end, revision, limit);
    }

    resp.SetJson(json::KvListToJson(kvs, kvs.size()));
    return resp;
}

// Watch 操作

HttpResponse EtcdServer::Watch(const std::string& key, Revision start_rev, bool prefix) {
    HttpResponse resp;

    int64_t watch_id = watch_mgr_->Watch(key, start_rev, prefix);
    auto watcher = watch_mgr_->GetWatcher(watch_id);

    if (!watcher) {
        resp.SetError(500, "failed to create watcher");
        return resp;
    }

    // 等待第一个事件 (简化实现)
    auto event = watcher->WaitForEvent(30000); // 30秒超时

    std::ostringstream oss;
    oss << "{";
    oss << "\"watch_id\":" << watch_id << ",";
    oss << "\"events\":[" << json::WatchEventToJson(event) << "]";
    oss << "}";
    resp.SetJson(oss.str());
    return resp;
}

HttpResponse EtcdServer::WatchCancel(LeaseId watch_id) {
    HttpResponse resp;

    if (watch_mgr_->Cancel(watch_id)) {
        std::ostringstream oss;
        oss << "{\"watch_id\":" << watch_id << ",\"canceled\":true}";
        resp.SetJson(oss.str());
    } else {
        resp.SetError(404, "watcher not found");
    }

    return resp;
}

// Lease 操作

HttpResponse EtcdServer::LeaseGrant(int64_t ttl_seconds) {
    HttpResponse resp;

    LeaseId id = lease_mgr_->Grant(ttl_seconds * 1000);
    resp.SetJson(json::LeaseToJson(id, ttl_seconds * 1000));
    return resp;
}

HttpResponse EtcdServer::LeaseRevoke(LeaseId id) {
    HttpResponse resp;

    if (!lease_mgr_->Revoke(id)) {
        resp.SetError(404, "lease not found");
        return resp;
    }

    std::ostringstream oss;
    oss << "{\"header\":{},\"revoked\":true}";
    resp.SetJson(oss.str());
    return resp;
}

HttpResponse EtcdServer::LeaseRenew(LeaseId id) {
    HttpResponse resp;

    auto lease = lease_mgr_->Get(id);
    if (!lease) {
        resp.SetError(404, "lease not found");
        return resp;
    }

    lease_mgr_->Renew(id);
    resp.SetJson(json::LeaseToJson(id, lease->ttl_ms));
    return resp;
}

HttpResponse EtcdServer::LeaseKeepAlive(LeaseId id) {
    return LeaseRenew(id); // KeepAlive 和 Renew 类似
}

// 集群信息

HttpResponse EtcdServer::ClusterInfo() {
    HttpResponse resp;

    auto state = raft_node_->State();
    std::string state_str;
    switch (state) {
    case NodeState::Follower: state_str = "Follower"; break;
    case NodeState::Candidate: state_str = "Candidate"; break;
    case NodeState::Leader: state_str = "Leader"; break;
    }

    std::ostringstream oss;
    oss << "{";
    oss << "\"node_id\":" << config_.node_id << ",";
    oss << "\"name\":\"" << config_.name << "\",";
    oss << "\"state\":\"" << state_str << "\",";
    oss << "\"term\":" << raft_node_->CurrentTerm() << ",";
    oss << "\"leader_id\":" << raft_node_->LeaderId() << ",";
    oss << "\"revision\":" << kvstore_->CurrentRevision() << ",";
    oss << "\"keys\":" << kvstore_->Keys().size();
    oss << "}";
    resp.SetJson(oss.str());
    return resp;
}

} // namespace myetcd