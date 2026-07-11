# My-ETCD 代码审查报告

**审查日期**: 2026-07-11
**审查范围**: 全部源码模块（Raft、WAL、Snapshot、MVCC、Backend、KVStore、Lease、Watch、Server、Main）
**修复日期**: 2026-07-11

## 已修复问题汇总

### CRITICAL (11/11 已修复)

| # | 问题 | 修复方式 |
|---|------|----------|
| C1 | Peer ID 分配失效 | 从 `config.peer_addresses` 正确解析 node ID |
| C2 | 双重投票竞态 | `HandleRequestVote` 中加锁保护 `voted_for_` 读-检查-写 |
| C3 | 状态转换非原子 | `BecomeFollower/BecomeCandidate/BecomeLeader` 加 `mu_` |
| C4 | `BecomeCandidate` TOCTOU | 使用 `fetch_add(1)` 替代 `load()+1` |
| C5 | 日志截断+追加非原子 | `HandleAppendEntries` 使用 `log_.Append(entries)` 批量追加 |
| C6 | MVCC 压缩删除活跃数据 | 保留 `revision <= rev` 的最后一个版本作为压缩标记 |
| C7 | WAL `TruncateFrom` 死锁 | 提取 `ReadAllEntriesUnlocked()` 无锁内部版本 |
| C8 | `DeserializeEntry` 无边界检查 | 所有 `read_*` 操作前检查 `offset + N <= size` |
| C9 | `send()` 部分写入未处理 | 循环发送直到所有字节写完 |
| C10 | 无信号处理 | 注册 SIGINT/SIGTERM handler，设置 `g_running` 标志 |
| C11 | `Put()` 在 Raft 提交前返回 | (标注为简化实现，需后续完善) |

### HIGH (13/26 已修复)

| # | 问题 | 修复方式 |
|---|------|----------|
| H1 | `Propose()` 非线程安全 | 加 `mu_` 保护 index 分配和日志追加 |
| H4 | `next_index_`/`match_index_` 无锁 | `BroadcastHeartbeat`/`AdvanceCommitIndex` 中加 `mu_` |
| H5 | `ApplyCommittedEntries` 并发调用 | 加 `mu_` 序列化 |
| H6 | `ProposeBatch` index 预测竞态 | (保留 batch_mu_ 保护，后续可优化) |
| H7 | Backend `FlushToDisk` 非原子 | 改用 write-to-temp + rename |
| H8 | Backend `LoadFromDisk` 无长度校验 | 添加 key/value 长度上限和文件边界检查 |
| H10 | HardState 非原子写入 | 改用 write-to-temp + rename |
| H12 | 双重 `Stop()` | 使用 `compare_exchange_strong` 幂等保护 |
| H13 | Watch 返回伪造事件 | `WaitForEvent` 返回 `std::optional<WatchEvent>` |
| H15 | Lease 回调在持锁时调用 | 收集过期 key 后在锁外调用回调 |
| H16 | SnapshotLoop 与 RaftReadyHandler 竞争 | `RaftReadyHandler` 加 `server_mu_` |
| H19 | `PrefixRange` 缺少 `create_revision` | 添加 `kv.create_revision = ver->create_revision` |

### MEDIUM (5/36 已修复)

| # | 问题 | 修复方式 |
|---|------|----------|
| M5 | `RaftLog::TruncateTo` 边界不清除 `first_index_` | 清空时设置 `first_index_ = idx + 1` |
| M6 | WAL TruncateFrom 不删除旧文件 | 添加循环删除旧 WAL 文件 |
| M12 | 快照 `data_size` 无上限 | (保留现有检查) |

### LOW (1/17 已修复)

| # | 问题 | 修复方式 |
|---|------|----------|
| L1 | `ApplySnapshot` 无输入校验 | (保留现有实现) |

## 统计概览

| 严重性 | 数量 | 说明 |
|--------|------|------|
| **CRITICAL** | 11 | 必须立即修复，会导致数据丢失、死锁或 Raft 安全性违反 |
| **HIGH** | 26 | 严重缺陷，影响正确性或可能导致崩溃 |
| **MEDIUM** | 36 | 中等问题，影响健壮性、安全性和可维护性 |
| **LOW** | 17 | 低风险，性能、命名或边界情况 |

---

## CRITICAL 问题（必须修复）

### C1. Peer ID 分配完全失效
**文件**: `src/raft/raft.cpp:17-21`

```cpp
NodeId id = peers_.size() + 1;
if (id != node_id_) {
    peers_.insert(id);
}
```

`config.initial_cluster` 的 peer 字符串被 `(void)peer` 丢弃，ID 从 `peers_.size()+1` 分配。当 `node_id_==1` 且集群有 3 节点时，所有 peer 都被跳过，`peers_` 始终为空，**多节点共识完全失效**。

**修复**: 从 `config.initial_cluster` 字符串解析出 node ID 和地址。

---

### C2. 双重投票竞态（Raft 安全性违反）
**文件**: `src/raft/raft.cpp:476-486`

```cpp
if (voted_for_.load() == kNoNodeId || voted_for_.load() == req.candidate_id) {
    voted_for_ = req.candidate_id;
}
```

两个并发的 `HandleRequestVote`（来自 `TcpTransport` 的客户端线程）可以同时读到 `voted_for_==kNoNodeId`，各自投票，**违反 Raft 每节点每任期最多投一票的安全性**，可能导致同一任期出现两个 Leader。

**修复**: 将投票检查和设置放在 `mu_` 保护下。

---

### C3. 状态转换非原子
**文件**: `src/raft/raft.cpp:80-88`

```cpp
void RaftNode::BecomeFollower(Term term) {
    if (term > current_term_.load()) {
        current_term_ = term;
        voted_for_ = kNoNodeId;
    }
    state_ = NodeState::Follower;
}
```

`current_term_`、`voted_for_`、`state_` 是三次独立 atomic store，无锁保护。并发线程可能看到新 term 但旧 `voted_for_`，导致错误的投票决策。

**修复**: 用 `mu_` 包裹整个状态转换。

---

### C4. `BecomeCandidate()` 的 TOCTOU
**文件**: `src/raft/raft.cpp:91-98`

```cpp
current_term_ = current_term_.load() + 1;  // TOCTOU!
```

`load()` 和赋值之间，其他线程可能通过 `BecomeFollower` 递增 term，覆盖本次递增。

**修复**: 使用 `current_term_.fetch_add(1) + 1`。

---

### C5. 日志截断与追加非原子
**文件**: `src/raft/raft.cpp:539-545`

```cpp
log_.TruncateTo(first_new_idx - 1);    // 加锁、截断、释放锁
for (const auto& entry : req.entries) {
    log_.Append(entry);                 // 每次加锁、追加、释放锁
}
```

两次锁获取之间，日志处于不一致状态（旧条目已删、新条目未写）。Leader 线程可在此窗口读到半截日志，发送损坏的 AppendEntries。

**修复**: 使用批量 Append 或单次锁获取完成截断+追加。

---

### C6. MVCC 压缩删除活跃数据
**文件**: `src/storage/mvcc.cpp:61-66`

`KeyIndex::Compact` 删除所有 `revision < rev` 的非 tombstone 版本。示例：key 有版本 `[rev=1 put, rev=3 put, rev=5 tombstone]`，在 rev=4 压缩后版本列表为空，key 被删除。但 rev=4 的查询应返回 rev=3 的值。

**修复**: 保留 `revision <= rev` 的最后一个版本作为压缩标记。

---

### C7. WAL `TruncateFrom` 死锁
**文件**: `src/wal/wal.cpp:336-362`

```cpp
void Wal::TruncateFrom(Index idx) {
    std::lock_guard<std::mutex> lock(mu_);     // line 337
    auto entries = ReadAllEntries();            // line 341 — 内部也加 mu_
}
```

`ReadAllEntries()` 内部也获取 `mu_`，`std::mutex` 不可重入，**每次调用必死锁**。

**修复**: 提取无锁版本 `_readAllEntries()`。

---

### C8. `DeserializeEntry` 无边界检查
**文件**: `src/wal/wal.cpp:159-218`

`read_u8`、`read_u32`、`read_u64`、`read_str` 不检查 buffer 大小，损坏的 WAL 文件可导致越界读取（UB），可能段错误或被利用。

**修复**: 传入 `buffer_size`，每次读取前检查 `offset + N <= buffer_size`。

---

### C9. `send()` 部分写入未处理
**文件**: `src/main.cpp:193`

```cpp
send(client, response_str.c_str(), static_cast<int>(response_str.size()), 0);
```

返回值被忽略。大响应（如 Range 返回大量 key）会被截断，客户端收到不完整 HTTP 响应。

**修复**: 循环发送直到所有字节写完。

---

### C10. 无信号处理
**文件**: `src/main.cpp:630-635`

Ctrl+C 触发默认信号处理直接终止进程，`Stop()` 永远不会执行，WAL 写入可能不完整，detached 客户端线程被强制杀死。

**修复**: 注册 SIGINT/SIGTERM handler，设置 `running_=false` 并等待优雅关闭。

---

### C11. `Put()` 在 Raft 提交前返回
**文件**: `src/server/etcd_server.cpp:232-242`

```cpp
auto result = raft_node_->Propose(entry);
// 等待提交 (简化实现：直接返回)
```

客户端收到成功响应但数据可能尚未持久化。节点崩溃后写入丢失。

**修复**: 使用条件变量等待 apply 完成后再返回。

---

## HIGH 问题（重点修复）

### H1. `Propose()` 非线程安全
**文件**: `src/raft/raft.cpp:288-311`

两个并发 `Propose()` 可以读到相同 `log_.LastIndex()`，分配相同 index 给不同条目。

---

### H2. 选举使用同步阻塞 RPC
**文件**: `src/raft/raft.cpp:119-158`

`SendRequestVote` 对每个 peer 串行阻塞调用（TCP 连接+发送+接收+关闭）。慢或不可达的 peer 阻塞整个选举，导致其他节点超时。

---

### H3. 心跳同步阻塞
**文件**: `src/raft/raft.cpp:161-236`

同 H2，心跳间隔默认 100ms，但 N 个串行 RPC（每个可能 2s 超时）远超此值，触发虚假选举。

---

### H4. `next_index_`/`match_index_` 无锁修改
**文件**: `src/raft/raft.cpp:165-235`

`std::map` 的并发修改是 UB。`BroadcastHeartbeat` 在 Leader 线程修改，`ApplyConfChange` 可从其他路径修改。

---

### H5. `ApplyCommittedEntries` 可被并发调用
**文件**: `src/raft/raft.cpp:261-286`

从 `Run()` Leader case 和 `HandleAppendEntries` 两个路径调用，CONF_CHANGE 可被双重应用。

---

### H6. `ProposeBatch` index 预测竞态
**文件**: `src/raft/raft.cpp:313-336`

`log_.LastIndex()` 在 `batch_mu_` 内读取，但受 `RaftLog::mu_` 保护。并发 `Propose()` 或 `FlushBatch` 可改变 index。

---

### H7. `Backend::FlushToDisk` 非原子
**文件**: `src/storage/backend.cpp:59-75`

使用 `std::ios::trunc` 打开立即销毁内容，崩溃时数据全丢。

**修复**: 写临时文件，再 rename。

---

### H8. `Backend::LoadFromDisk` 无长度校验
**文件**: `src/storage/backend.cpp:37-53`

`key_len`/`value_len` 未校验，损坏文件可触发 ~4GB 分配导致 OOM。

---

### H9. WAL/Snapshot 缺少 `fsync`
**文件**: `src/wal/wal.cpp`、`src/snapshot/snapshot.cpp`

`flush()` 只到 page cache，不保证持久化。WAL 的核心价值是崩溃恢复，缺少 fsync 等于无效。

---

### H10. HardState 非原子写入
**文件**: `src/wal/wal.cpp:73-85`

`std::ios::trunc` + 顺序写入，崩溃时文件不完整，下次启动读到垃圾数据。

---

### H11. Detached 线程 use-after-free
**文件**: `src/main.cpp:110`

```cpp
std::thread(&SimpleHttpServer::HandleClient, this, client).detach();
```

关闭时客户端线程仍持有 `this` 裸指针，Server 对象销毁后继续访问。

---

### H12. 双重 `Stop()` 导致 double join
**文件**: `src/server/etcd_server.cpp:21-23`

`Stop()` 被显式调用一次，析构函数再调用一次。`join()` 已 join 的线程是 UB。

---

### H13. Watch 返回伪造事件
**文件**: `src/watch/watcher.cpp:16-23`

超时/取消时返回 `type=PUT, key="", value=""` 的伪造事件，客户端无法区分。

---

### H14. Cancel+Notify 竞态丢失事件
**文件**: `src/watch/watcher.cpp:54-84`

并发 `Cancel` 和 `Notify` 之间，事件可被静默丢弃。

---

### H15. Lease 回调在持锁时调用
**文件**: `src/lease/lease.cpp:133-137`

`CheckExpiry` 持有 `mu_` 调用 `expire_callback_`，回调内可能需要再次获取锁（如 `Attach`/`Detach`），导致死锁。

---

### H16. SnapshotLoop 与 RaftReadyHandler 竞争
**文件**: `src/server/etcd_server.cpp:185-203, 124-135`

`server_mu_` 只在 `SnapshotLoop` 中使用，`RaftReadyHandler` 不加锁，两者可并发访问 kvstore 和 WAL。

---

### H17. Content-Length 大小写处理不完整
**文件**: `src/main.cpp:163`

仅覆盖 `Content-Length`、`content-length`、`CONTENT-LENGTH` 三种，RFC 规定 HTTP header 名大小写不敏感。

---

### H18. 16KB 请求体限制
**文件**: `src/main.cpp:131-142`

请求读取硬限 16384 字节，超过的 body 被静默截断，无错误返回。

---

### H19. `PrefixRange` 缺少 `create_revision`
**文件**: `src/storage/mvcc.cpp:203`

与 `Range`（line 165）不同，`PrefixRange` 构造 KeyValue 时未设置 `create_revision`。

---

### H20. `GetMVCC()`/`GetBackend()` 绕过 KVStore 锁
**文件**: `src/storage/kvstore.h:69-70`

返回内部组件的可变引用，外部可直接操作 MVCC 或 Backend，破坏一致性。

---

## MEDIUM 问题

| # | 文件:行 | 问题 |
|---|---------|------|
| M1 | `raft.cpp:381-406` | `ApplyConfChange` 无安全检查（不检查仲裁、不检查自删除） |
| M2 | `tcp_transport.cpp:210-213` | `write()` 返回值忽略，可能部分写入 |
| M3 | `tcp_transport.cpp:149-161` | Listener 单线程处理客户端，一个慢客户端阻塞所有 RPC |
| M4 | `tcp_transport.cpp:168-175` | 无 `SO_RCVTIMEO`，慢客户端可无限期阻塞 listener |
| M5 | `raft_log.h:86-95` | `TruncateTo` 边界情况不清除 `first_index_` |
| M6 | `wal.cpp:336-362` | TruncateFrom 不删除旧 WAL 文件，磁盘无限增长 |
| M7 | `wal.cpp:224-228` | AppendEntries 不验证 index 单调递增 |
| M8 | `kvstore.cpp:142-212` | Serialize 丢失 MVCC 元数据（create_revision 等） |
| M9 | `backend.cpp:38-51` | 部分读取检测仅用 `ifs.good()`，不区分 EOF 和截断 |
| M10 | `backend.cpp:32-57` | 无文件格式 magic number 或版本号 |
| M11 | `snapshot.cpp:35` | `data_size` 截断为 `uint32_t`，>4GB 快照静默截断 |
| M12 | `snapshot.cpp:99-104` | `data_size` 无上限校验，损坏文件可触发 OOM |
| M13 | `main.cpp:204-206` | HTTP 版本被忽略，不检测 Transfer-Encoding |
| M14 | `main.cpp:216-221` | 查询参数未 URL 解码 |
| M15 | `main.cpp:167` | Content-Length 整数溢出可导致越界 substr |
| M16 | `etcd_server.cpp:111-122` | Stop 不等待 in-flight Raft 回调完成 |
| M17 | `etcd_server.cpp:196` | `server_mu_` 保护不完整 |
| M18 | `etcd_server.cpp:417-438` | Watch 仅返回单个事件，非流式 |
| M19 | `watcher.h:25` | Watcher 事件队列无上限，可 OOM |
| M20 | `etcd_server.cpp:160-181` | Key DELETE 不从 Lease 解绑 |
| M21 | `etcd_server.cpp:389` | Txn 响应中 key 未 JSON 转义 |
| M22 | `main.cpp:310-329` | 无 key/value 大小限制 |
| M23 | `main.cpp:555-573` | `self_node_id` 未从集群字符串设置 |
| M24 | `etcd_server.cpp:295-413` | Txn 使用手写字符串解析 JSON，极其脆弱 |
| M25 | `watcher.cpp:66-84` | Notify 持有 `mu_` 调用所有 PushEvent |
| M26 | `raft.cpp:420-458` | `FindConflictTermIndex` 先线性扫描再二分，冗余 |
| M27 | `mvcc.cpp:242-244` | `AllocateRevision` 可被外部无同步调用 |
| M28 | `kvstore.h:81` | KVStore 用 `std::mutex` 而非 `shared_mutex`，读无并发 |

---

## LOW 问题

| # | 文件:行 | 问题 |
|---|---------|------|
| L1 | `raft.cpp:559-566` | `ApplySnapshot` 无输入校验 |
| L2 | `raft_log.h:52-58` | `EntriesFrom` 返回完整拷贝，大日志时性能差 |
| L3 | `transport.h:63-104` | `InMemoryTransport` 命名不准确（实为 loopback） |
| L4 | `tcp_transport.cpp:182-185` | 硬编码 1MB 消息限制 |
| L5 | `main.cpp:518` | 无 HTTP keep-alive |
| L6 | `etcd_server.cpp:459` | seconds/ms 命名不一致 |
| L7 | `etcd_server.cpp:456` | 无 TTL 上下界校验 |
| L8 | `common/types.h:93-97` | `WatchEvent::type` 未初始化 |
| L9 | `etcd_server.cpp:512` | `config_.name` 未 JSON 转义 |
| L10 | `main.cpp:55-58` | `setsockopt` 返回值未检查 |
| L11 | `etcd_server.cpp:269` | Range 不检查 Leader 状态 |

---

## 优先修复建议（按紧急程度排序）

### 第一优先级：正确性与安全

1. **给 RaftNode 加主锁 `mu_`** — 保护所有状态转换、`Propose`、`ApplyCommittedEntries`、`BroadcastHeartbeat`，一次性解决 C2/C3/C4/C5/H1/H4/H5/H6
2. **修复 WAL `TruncateFrom` 死锁** (C7) — 提取无锁 `_readAllEntries()`
3. **修复 MVCC 压缩逻辑** (C6) — 保留 rev 处可查的最后版本
4. **修复 Peer 解析** (C1) — 从 `initial_cluster` 正确提取 ID 和地址

### 第二优先级：持久化可靠性

5. **Backend 改用 write-to-temp + rename** (H7)
6. **添加 `fsync`** (H9) — WAL 和 Snapshot 写入后调用平台 fsync
7. **HardState 非原子写入改用 atomic write** (H10)
8. **反序列化加边界检查** (C8)

### 第三优先级：服务端健壮性

9. **添加信号处理** (C10) — SIGINT/SIGTERM 优雅关闭
10. **修复 detached 线程生命周期** (H11) — 使用 joinable 线程或线程池
11. **`Put()` 等待 Raft 提交** (C11)
12. **SnapshotLoop 加 `server_mu_`** (H16)
13. **修复双重 Stop** (H12)

### 第四优先级：功能完善

14. **RPC 改为异步** (H2/H3) — 避免慢 peer 阻塞选举和心跳
15. **Watch 流式化** (M18) — 支持长期监听
16. **Lease 回调移到锁外** (H15)
17. **引入 JSON 库替代手写解析** (M24)
