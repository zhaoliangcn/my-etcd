# 更新日志

## [1.0.0] - 2026-07-01

### 🚀 初始版本

首个正式版本，实现了分布式键值存储系统的基础功能。

### ✨ 新增

- **Raft 共识协议**
  - Leader 选举（随机超时防脑裂）
  - 日志复制（AppendEntries RPC）
  - 投票机制（RequestVote RPC）
  - 状态转换：Follower ↔ Candidate ↔ Leader

- **存储引擎**
  - MVCC 多版本并发控制（内存 B-Tree 索引）
  - 基于文件的持久化 Backend 存储
  - 支持按 revision 查询历史版本
  - 范围查询和前缀查询

- **WAL（预写日志）**
  - 二进制格式持久化日志
  - 启动时自动回放 WAL 恢复状态
  - 自定义文件管理

- **快照（Snapshot）**
  - 定期日志压缩（可配置间隔）
  - 完整 KV 数据序列化快照
  - 快照恢复机制

- **Watch 机制**
  - 单 key 和前匹配监听
  - 事件队列与推送
  - 支持指定起始 revision
  - 超时处理

- **Lease（租约）**
  - 租约创建、撤销、续约
  - TTL 自动过期与 key 清理
  - KeepAlive 机制

- **HTTP REST API**
  - 纯原生 socket 实现，无外部 HTTP 库依赖
  - 完整的 KV / Watch / Lease API
  - JSON 格式响应

- **单节点运行**
  - 一键启动，开箱即用
  - 可配置监听地址、端口、数据目录

- **测试**
  - C++ 单元测试（MVCC、Backend、KVStore、Lease、Watch、Raft）
  - Python 集成测试（KV CRUD、Watch、Lease）
  - Pytest fixtures 自动化测试框架
