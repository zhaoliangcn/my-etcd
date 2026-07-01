#include "server/etcd_server.h"
#include "raft/tcp_transport.h"
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
    raft_node_->SetConfChangeHandler([this](const ConfChange& cc) {
        OnConfChangeApplied(cc);
    });

    // 6.5 设置 TCP Transport（多节点模式）
    if (!config_.peer_addresses.empty()) {
        auto transport = std::make_shared<TcpTransport>(config_.node_id, config_.listen_peer_addr);
        transport->SetPeerAddresses(config_.peer_addresses);
        transport->SetRequestVoteHandler(
            std::bind(&RaftNode::HandleRequestVote, raft_node_.get(), std::placeholders::_1));
        transport->SetAppendEntriesHandler(
            std::bind(&RaftNode::HandleAppendEntries, raft_node_.get(), std::placeholders::_1));
        transport->Start();
        raft_node_->SetTransport(transport);
        transport_ = transport;
        std::cout << "[Server] TCP Transport started on " << config_.listen_peer_addr << std::endl;
    }

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
    if (transport_) transport_->Stop();
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

HttpResponse EtcdServer::Txn(const std::string& request_body) {
    HttpResponse resp;

    // 简化 JSON 解析：只支持最基本的格式
    // {"compare":[{"target":"value","key":"k","op":"equal","value":"v"}],
    //  "success":[{"type":"PUT","key":"k","value":"v"}],
    //  "failure":[{"type":"DELETE","key":"k"}]}

    auto parse_str = [](const std::string& s, const std::string& key) -> std::string {
        std::string search = "\"" + key + "\":\"";
        size_t p = s.find(search);
        if (p == std::string::npos) return "";
        p += search.size();
        size_t end = s.find('"', p);
        if (end == std::string::npos) return "";
        return s.substr(p, end - p);
    };

    // 检查 compare 条件
    bool conditions_met = true;

    // 查找 compare 数组
    size_t cmp_start = request_body.find("\"compare\":[");
    if (cmp_start != std::string::npos) {
        size_t cmp_end = request_body.find(']', cmp_start);
        std::string cmp_section = request_body.substr(cmp_start, cmp_end - cmp_start + 1);

        // 遍历 compare 数组中的每个条件
        size_t pos = cmp_section.find('{', 0);
        while (pos != std::string::npos) {
            size_t brace_end = cmp_section.find('}', pos);
            if (brace_end == std::string::npos) break;
            std::string cmp_obj = cmp_section.substr(pos, brace_end - pos + 1);

            std::string target = parse_str(cmp_obj, "target");
            std::string key = parse_str(cmp_obj, "key");
            std::string op_str = parse_str(cmp_obj, "op");
            std::string cmp_value = parse_str(cmp_obj, "value");

            // 获取当前 key 的值
            auto kv = kvstore_->Get(key);
            std::string current_value = kv ? kv->value : "";

            // 只支持值比较（equal / notequal）
            CompareOp op = (op_str == "notequal") ? CompareOp::NotEqual : CompareOp::Equal;

            if (target == "value") {
                if (op == CompareOp::Equal) {
                    if (current_value != cmp_value) conditions_met = false;
                } else {
                    if (current_value == cmp_value) conditions_met = false;
                }
            } else if (target == "version") {
                int64_t cur_ver = kv ? kv->version : 0;
                int64_t exp_ver = 0;
                try { exp_ver = std::stoll(cmp_value); } catch (...) {}
                if (op == CompareOp::Equal && cur_ver != exp_ver) conditions_met = false;
                if (op == CompareOp::NotEqual && cur_ver == exp_ver) conditions_met = false;
            } else {
                // 不支持的条件类型
                conditions_met = false;
            }

            pos = cmp_section.find('{', brace_end + 1);
        }
    }

    // 选择执行成功或失败分支
    auto ops_to_execute = conditions_met ? "success" : "failure";

    // 查找执行数组
    std::string exec_search = "\"" + std::string(ops_to_execute) + "\":[";
    size_t exec_start = request_body.find(exec_search);
    if (exec_start == std::string::npos) {
        resp.SetJson("{\"succeeded\":" + std::string(conditions_met ? "true" : "false") + ",\"results\":[]}");
        return resp;
    }

    // 解析并执行操作
    std::vector<std::string> results;
    size_t exec_arr_start = request_body.find('[', exec_start);
    size_t exec_arr_end = request_body.find(']', exec_arr_start);
    std::string exec_section = request_body.substr(exec_arr_start, exec_arr_end - exec_arr_start + 1);

    size_t op_pos = exec_section.find('{', 0);
    while (op_pos != std::string::npos) {
        size_t op_end = exec_section.find('}', op_pos);
        if (op_end == std::string::npos) break;
        std::string op_obj = exec_section.substr(op_pos, op_end - op_pos + 1);

        std::string op_type = parse_str(op_obj, "type");
        std::string op_key = parse_str(op_obj, "key");
        std::string op_value = parse_str(op_obj, "value");

        if (op_type == "PUT" && !op_key.empty()) {
            // 通过 Raft 提交 Put
            RaftEntry entry;
            entry.type = EventType::PUT;
            entry.key = op_key;
            entry.value = op_value;
            raft_node_->Propose(entry);

            results.push_back("{\"type\":\"PUT\",\"key\":\"" + op_key + "\"}");
        } else if (op_type == "DELETE" && !op_key.empty()) {
            RaftEntry entry;
            entry.type = EventType::DELETE;
            entry.key = op_key;
            raft_node_->Propose(entry);

            results.push_back("{\"type\":\"DELETE\",\"key\":\"" + op_key + "\"}");
        }

        op_pos = exec_section.find('{', op_end + 1);
    }

    // 构建响应
    std::ostringstream oss;
    oss << "{\"succeeded\":" << (conditions_met ? "true" : "false")
        << ",\"results\":[";
    for (size_t i = 0; i < results.size(); ++i) {
        if (i > 0) oss << ",";
        oss << results[i];
    }
    oss << "]}";
    resp.SetJson(oss.str());
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

// 成员变更

void EtcdServer::OnConfChangeApplied(const ConfChange& cc) {
    if (cc.type == ConfChangeType::AddNode && transport_) {
        transport_->SetPeerAddress(cc.node_id, cc.peer_addr);
        std::cout << "[Server] ConfChange applied: Add Node " << cc.node_id
                  << " at " << cc.peer_addr << std::endl;
    } else if (cc.type == ConfChangeType::RemoveNode && transport_) {
        std::cout << "[Server] ConfChange applied: Remove Node " << cc.node_id << std::endl;
    }
}

HttpResponse EtcdServer::MemberAdd(const std::string& peer_addr) {
    HttpResponse resp;
    if (!raft_node_->IsLeader()) {
        resp.SetError(503, "not leader");
        return resp;
    }

    NodeId new_id = 1;
    auto peers = raft_node_->GetPeers();
    while (peers.count(new_id) > 0 || new_id == config_.node_id) {
        ++new_id;
    }

    ConfChange cc;
    cc.type = ConfChangeType::AddNode;
    cc.node_id = new_id;
    cc.peer_addr = peer_addr;

    auto result = raft_node_->ProposeConfChange(cc);
    if (!result.accepted) {
        resp.SetError(500, result.error);
        return resp;
    }

    std::ostringstream oss;
    oss << "{\"header\":{\"revision\":0},\"member\":{"
        << "\"ID\":" << new_id << ","
        << "\"peerURLs\":[\"" << peer_addr << "\"]}}";
    resp.SetJson(oss.str());
    return resp;
}

HttpResponse EtcdServer::MemberRemove(NodeId id) {
    HttpResponse resp;
    if (!raft_node_->IsLeader()) {
        resp.SetError(503, "not leader");
        return resp;
    }

    if (id == config_.node_id) {
        resp.SetError(400, "cannot remove self");
        return resp;
    }

    auto peers = raft_node_->GetPeers();
    if (peers.count(id) == 0) {
        resp.SetError(404, "member not found");
        return resp;
    }

    ConfChange cc;
    cc.type = ConfChangeType::RemoveNode;
    cc.node_id = id;

    auto result = raft_node_->ProposeConfChange(cc);
    if (!result.accepted) {
        resp.SetError(500, result.error);
        return resp;
    }

    std::ostringstream oss;
    oss << "{\"header\":{\"revision\":0},\"member\":{\"ID\":" << id << "}}";
    resp.SetJson(oss.str());
    return resp;
}

} // namespace myetcd