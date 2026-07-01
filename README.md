# My-ETCD

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![C++17](https://img.shields.io/badge/C++-17-blue)](https://en.cppreference.com/w/cpp/17)
[![Build](https://img.shields.io/badge/build-CMake-green)](CMakeLists.txt)

My-ETCD 是一个使用 **C++17** 从零构建的轻量级分布式键值存储系统，参考 [etcd](https://etcd.io/) 的核心设计，实现了 Raft 共识协议、MVCC 多版本并发控制、Watch 机制和租约管理等核心功能。

## ✨ 特性

- **Raft 共识协议** — Leader 选举、日志复制、安全保证
- **MVCC 存储引擎** — 多版本并发控制，支持按 revision 查询历史
- **Watch 机制** — 监听键值变化，支持前缀匹配和事件推送
- **Lease（租约）** — 键值 TTL 自动过期管理
- **WAL（预写日志）** — 写入前日志，保障数据持久性
- **快照（Snapshot）** — 日志压缩，防止 WAL 无限增长
- **HTTP REST API** — 纯 socket 实现，无外部 HTTP 库依赖
- **单节点运行** — 支持单机一键启动，开箱即用

## 🚀 快速开始

### 环境要求

- CMake ≥ 3.16
- C++17 兼容的编译器（GCC 8+, Clang 7+, MSVC 2019+）
- 可选：Protobuf + gRPC（启用 gRPC 支持）

### 构建

```bash
# 克隆项目
git clone <your-repo-url>
cd my-etcd

# 创建构建目录
mkdir build && cd build

# 配置
cmake ..

# 编译
cmake --build .

# 运行单元测试（可选）
ctest --output-on-failure
```

### 启动服务

```bash
# 单节点启动（默认监听 0.0.0.0:2379）
./build/my-etcd

# 自定义端口和数据目录
./build/my-etcd --name node1 --data-dir ./mydata --listen-addr 127.0.0.1:2379
```

### 使用示例

```bash
# 写入键值
curl -X PUT "http://127.0.0.1:2379/v3/kv/put?key=foo" -d "bar"

# 读取单个键
curl -X POST "http://127.0.0.1:2379/v3/kv/range?key=foo"

# 范围查询
curl -X POST "http://127.0.0.1:2379/v3/kv/range?key=a&range_end=z"

# 删除键
curl -X POST "http://127.0.0.1:2379/v3/kv/delete?key=foo"

# 创建租约（TTL=60秒）
curl -X POST "http://127.0.0.1:2379/v3/lease/grant?ttl=60"

# 查看集群信息
curl "http://127.0.0.1:2379/v3/cluster/info"
```

## 📚 API 参考

| 方法 | 端点 | 说明 |
|------|------|------|
| PUT | `/v3/kv/put?key={key}&lease_id={id}` | 写入键值对 |
| POST | `/v3/kv/range?key={key}&range_end={end}&limit={n}&revision={rev}` | 范围查询 |
| POST | `/v3/kv/delete?key={key}` | 删除键值对 |
| POST | `/v3/watch?key={key}&prefix={bool}&start_rev={rev}` | 监听键变化 |
| POST | `/v3/watch/cancel?ID={id}` | 取消监听 |
| POST | `/v3/lease/grant?ttl={seconds}` | 创建租约 |
| POST | `/v3/lease/revoke?ID={id}` | 撤销租约 |
| POST | `/v3/lease/keepalive?ID={id}` | 续约 |
| GET | `/v3/cluster/info` | 集群状态信息 |

## 🏗️ 项目架构

```
my-etcd/
├── CMakeLists.txt           # 构建配置
├── DESIGN.md                # 详细设计文档
├── LICENSE                  # Apache 2.0 许可证
├── README.md                # 本文件
├── src/
│   ├── main.cpp             # 入口 & HTTP Server
│   ├── common/types.h       # 公共类型定义
│   ├── raft/                # Raft 共识模块
│   ├── wal/                 # 预写日志模块
│   ├── snapshot/            # 快照模块
│   ├── storage/             # 存储引擎 (MVCC + Backend)
│   ├── lease/               # 租约管理模块
│   ├── watch/               # Watch 监听模块
│   └── server/              # 服务端整合 & HTTP 处理
└── tests/                   # 单元测试和集成测试
    ├── cpp/                 # C++ 单元测试
    ├── conftest.py          # Pytest fixtures
    └── test_*.py            # Python 集成测试
```

详细设计请参阅 [DESIGN.md](DESIGN.md)。

## 🧪 测试

```bash
# C++ 单元测试
cd build && ctest --output-on-failure

# Python 集成测试
cd tests && pip install -r requirements.txt && python run_tests.py
```

## 🗺️ 待实现

- [ ] 多节点集群通信（TCP Transport）
- [ ] 成员变更（Add/Remove Node）
- [ ] 认证鉴权
- [ ] 事务（Txn）
- [ ] 完整的 gRPC 支持
- [ ] 性能优化（批量提交、流水线）

## 🤝 贡献

欢迎贡献代码！请阅读 [CONTRIBUTING.md](CONTRIBUTING.md) 了解贡献流程。

## 📄 许可

本项目基于 [Apache License 2.0](LICENSE) 开源。
