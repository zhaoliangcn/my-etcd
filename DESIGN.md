# My-ETCD 设计文档

## 1. 项目概述

My-ETCD 是一个使用 C++17 从零构建的分布式键值存储系统，参考 etcd 的核心设计，实现了 Raft 共识协议、MVCC 存储引擎、Watch 机制、租约管理等核心功能。

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Client API Layer                        │
│  ┌──────────────────────┐  ┌──────────────────────────────┐ │
│  │   HTTP REST API      │  │   gRPC API (可选)            │ │
│  │   (GET/PUT/DELETE    │  │   (Range/Put/DeleteRange     │ │
│  │    /watch)           │  │    /Watch/Lease)             │ │
│  └──────────┬───────────┘  └──────────────┬───────────────┘ │
└─────────────┼──────────────────────────────┼─────────────────┘
              │                              │
┌─────────────┼──────────────────────────────┼─────────────────┐
│             ▼               Server Core    ▼                 │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                   EtcdServer                          │   │
│  │  ┌─────────┐ ┌──────────┐ ┌─────────┐ ┌──────────┐  │   │
│  │  │  Lease   │ │  Watch   │ │  Auth   │ │  Cluster │  │   │
│  │  │ Manager  │ │ Manager  │ │ Manager │ │ Manager  │  │   │
│  │  └────┬─────┘ └────┬─────┘ └─────────┘ └──────────┘  │   │
│  └───────┼─────────────┼────────────────────────────────┘   │
└──────────┼─────────────┼────────────────────────────────────┘
           │             │
┌──────────┼─────────────┼────────────────────────────────────┐
│          ▼             ▼        Storage Layer                │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                     KVStore                           │   │
│  │  ┌──────────────────┐  ┌──────────────────────────┐  │   │
│  │  │   MVCC Index      │  │   Backend Storage        │  │   │
│  │  │  (B-Tree 内存索引) │  │  (文件持久化 Key-Value)  │  │   │
│  │  └──────────────────┘  └──────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
           │
┌──────────┼──────────────────────────────────────────────────┐
│          ▼              Consensus Layer                      │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                   RaftNode                            │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────────────────┐  │   │
│  │  │ RaftLog  │ │Transport │ │ Snapshot Manager     │  │   │
│  │  │(内存日志) │ │(RPC通信) │ │ (日志压缩)           │  │   │
│  │  └────┬─────┘ └──────────┘ └──────────────────────┘  │   │
│  └───────┼──────────────────────────────────────────────┘   │
└──────────┼──────────────────────────────────────────────────┘
           │
┌──────────┼──────────────────────────────────────────────────┐
│          ▼              Persistence Layer                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │   WAL (Write-Ahead Log)    │   Snapshot Files        │   │
│  │   /data/wal/wal_*.log      │   /data/snap/snapshot_* │   │
│  └────────────────────────────┴─────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## 3. 模块设计

### 3.1 Raft 共识模块 (`src/raft/`)

**职责**: 实现 Raft 共识算法，保证分布式一致性。

**核心类型**:
- `RaftNode`: Raft 节点主类，管理状态机（Follower/Candidate/Leader）
- `RaftLog`: 日志管理，存储和检索 Raft 日志条目
- `Transport`: 传输层抽象，处理节点间 RPC 通信

**状态转换**:
```
        超时/开始选举          获得多数票
Follower ────────────► Candidate ──────────► Leader
    ▲                      │                     │
    │    发现更高任期       │    发现更高任期       │
    └──────────────────────┴─────────────────────┘
```

**关键 RPC**:
- `RequestVote`: Candidate 请求投票
- `AppendEntries`: Leader 复制日志/心跳

**选举超时**: 随机化在 `[election_timeout_ms, max_election_timeout_ms]` 之间，防止脑裂。

### 3.2 WAL 模块 (`src/wal/`)

**职责**: 提供预写日志，保证数据持久性。

**文件格式**:
```
/data/wal/wal_0.log
/data/wal/wal_1.log
...
```

每条记录格式:
```
[4字节长度][1字节类型][8字节term][8字节index][1字节操作类型][key长度][key][value长度][value][8字节lease_id]
```

**操作**:
- `Open/Close`: 打开/关闭 WAL
- `AppendEntries`: 追加日志
- `ReadAllEntries`: 读取所有日志（用于恢复）
- `SaveSnapshot/LoadSnapshot`: 快照持久化
- `TruncateFrom`: 截断日志（日志压缩后）

### 3.3 快照模块 (`src/snapshot/`)

**职责**: 日志压缩，防止 WAL 无限增长。

**触发时机**:
- 定期触发（`snapshot_interval` 配置项）
- 日志条目数超过阈值

**快照内容**:
- 最后应用的日志索引和任期
- 完整的 KV 数据序列化快照

### 3.4 存储模块 (`src/storage/`)

**职责**: 提供 MVCC 键值存储。

**子模块**:

#### 3.4.1 Backend (`backend.h/cpp`)
- 底层持久化存储，使用文件存储键值对
- 支持批量写入和范围查询
- 使用简单的文件格式存储

#### 3.4.2 MVCC (`mvcc.h/cpp`)
- 多版本并发控制
- 每个 key 维护一个版本历史（KeyIndex）
- 支持按 revision 查询历史版本

```
KeyIndex 结构:
"foo" -> {
    generations: [
        {rev: 1, ver: 1},   // 创建
        {rev: 3, ver: 2},   // 更新
        {rev: 5, ver: 3},   // 更新
        {tombstone: rev: 7}  // 删除
    ]
}
```

#### 3.4.3 KVStore (`kvstore.h/cpp`)
- 对外暴露的 KV 操作接口
- 协调 MVCC 和 Backend
- 提供 Range、Put、Delete、Compact 操作

### 3.5 租约模块 (`src/lease/`)

**职责**: 管理键的 TTL 过期。

**核心功能**:
- `Grant(ttl)`: 创建租约，返回 LeaseID
- `Revoke(id)`: 撤销租约
- `Renew(id)`: 续约
- 定期检查过期租约，删除关联的 key

**数据结构**:
```
leases_: map<LeaseId, Lease>
    Lease {
        id, ttl, remaining_ttl, 
        attached_keys: set<string>
    }
```

### 3.6 Watch 模块 (`src/watch/`)

**职责**: 监听键的变化，实时推送事件。

**核心功能**:
- `Watch(key, start_rev)`: 创建 watcher
- 事件类型: PUT、DELETE
- 支持前缀匹配
- 事件通道: 每个 watcher 有一个事件队列

```
Watcher {
    id, key, prefix_match, start_rev,
    event_channel: queue<WatchEvent>
}
```

### 3.7 服务端模块 (`src/server/`)

**职责**: 对外提供 API 服务。

**API 端点**:
```
PUT    /v3/kv/put         - 写入键值
POST   /v3/kv/range       - 范围查询
POST   /v3/kv/delete      - 删除键值
POST   /v3/kv/txn         - 事务
POST   /v3/watch          - 监听
POST   /v3/lease/grant    - 创建租约
POST   /v3/lease/revoke   - 撤销租约
POST   /v3/lease/keepalive - 续约
```

## 4. 数据流

### 4.1 写入流程
```
Client PUT /v3/kv/put
    │
    ▼
EtcdServer::Put()
    │
    ▼
RaftNode::Propose()  ──► 广播到 Followers
    │
    ▼ (日志提交后)
KVStore::Put()
    ├──► MVCC::Put()  ──► 更新内存索引
    ├──► Backend::Put() ──► 持久化到文件
    ├──► Lease::Attach() ──► 关联租约
    └──► Watch::Notify() ──► 通知 Watchers
```

### 4.2 读取流程
```
Client POST /v3/kv/range
    │
    ▼
EtcdServer::Range()
    │
    ▼
KVStore::Range()
    ├──► MVCC::Range() ──► 从内存索引查找
    └──► Backend::Get() ──► 从持久化存储读取 value
```

### 4.3 启动恢复流程
```
main()
    │
    ▼
WAL::Open()
    ├──► LoadSnapshot()     ──► 恢复快照
    ├──► ReadAllEntries()   ──► 重放 WAL
    └──► LoadHardState()    ──► 恢复 Raft 状态
    │
    ▼
RaftNode::Start()
    │
    ▼
EtcdServer::Start() ──► 启动 HTTP/gRPC 服务
```

## 5. 目录结构

```
my-etcd/
├── CMakeLists.txt           # 构建配置
├── DESIGN.md                # 设计文档
├── src/
│   ├── main.cpp             # 入口
│   ├── common/
│   │   └── types.h          # 公共类型定义
│   ├── raft/
│   │   ├── raft.h           # Raft 模块入口
│   │   ├── raft.cpp         # RaftNode 实现
│   │   ├── raft_node.h      # RaftNode 声明
│   │   ├── raft_log.h       # RaftLog 声明
│   │   ├── raft_log.cpp     # RaftLog 实现
│   │   └── transport.h      # 传输层抽象
│   ├── wal/
│   │   ├── wal.h            # WAL 声明
│   │   └── wal.cpp          # WAL 实现
│   ├── snapshot/
│   │   ├── snapshot.h       # 快照声明
│   │   └── snapshot.cpp     # 快照实现
│   ├── storage/
│   │   ├── backend.h        # 后端存储声明
│   │   ├── backend.cpp      # 后端存储实现
│   │   ├── mvcc.h           # MVCC 声明
│   │   ├── mvcc.cpp         # MVCC 实现
│   │   ├── kvstore.h        # KVStore 声明
│   │   └── kvstore.cpp      # KVStore 实现
│   ├── lease/
│   │   ├── lease.h          # 租约声明
│   │   └── lease.cpp        # 租约实现
│   ├── watch/
│   │   ├── watcher.h        # Watch 声明
│   │   └── watcher.cpp      # Watch 实现
│   └── server/
│       ├── etcd_server.h    # 服务端声明
│       ├── etcd_server.cpp  # 服务端实现
│       ├── http_handler.h   # HTTP 处理声明
│       └── http_handler.cpp # HTTP 处理实现
└── data/                    # 运行时数据目录
    ├── hardstate            # Raft 硬状态
    ├── wal/                 # WAL 文件
    │   └── wal_*.log
    └── snap/                # 快照文件
        └── snapshot_*
```

## 6. 并发模型

- **Raft 主循环**: 独立线程，运行状态机
- **KVStore**: 内部使用 `std::shared_mutex`，读共享写独占
- **Watch**: 事件通知使用 `std::mutex` + `std::condition_variable`
- **Lease**: 定期检查使用独立线程
- **HTTP Server**: 每个请求在独立线程处理
- **WAL**: 写入操作使用 `std::mutex` 保护

## 7. 关键设计决策

1. **C++17 标准**: 使用现代 C++ 特性（optional, variant, string_view, filesystem）
2. **无外部依赖**: 核心功能不依赖第三方库，gRPC 为可选依赖
3. **文件存储**: 使用自定义二进制格式替代 LevelDB/RocksDB，降低依赖
4. **内存 MVCC 索引**: 使用 `std::map` 实现 B-Tree 索引，key 的版本历史用 `std::vector` 存储
5. **简单 HTTP API**: 使用原始 socket + HTTP 解析，避免引入 HTTP 库依赖

## 8. 待实现/改进

- [ ] 多节点集群通信（TCP Transport）
- [ ] 成员变更（Add/Remove Node）
- [ ] 认证鉴权
- [ ] 事务（Txn）
- [ ] 完整的 gRPC 支持
- [ ] 性能优化（批量提交、流水线）
- [ ] 单元测试和集成测试