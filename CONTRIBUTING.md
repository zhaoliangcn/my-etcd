# 贡献指南

感谢您对 My-ETCD 的关注！我们欢迎各种形式的贡献，包括但不限于：报告 Bug、提交功能建议、改进文档、提交代码等。

## 行为准则

本项目遵守 [贡献者公约](CODE_OF_CONDUCT.md)。所有参与者都应遵守该准则，维护一个友好、包容的社区环境。

## 如何贡献

### 报告 Bug

1. 使用 GitHub Issues 提交 bug 报告
2. 请清晰描述问题，包括：
   - 环境信息（操作系统、编译器版本等）
   - 复现步骤
   - 期望行为和实际行为
   - 相关日志或错误输出

### 提交功能建议

1. 先在 Issues 中搜索是否已有类似建议
2. 如没有，新建一个 Issue，标题以 `[Feature]` 开头
3. 详细描述功能场景和预期行为

### 提交代码（Pull Request）

1. **Fork** 本仓库
2. 创建您的特性分支：`git checkout -b feature/my-feature`
3. 编写代码并添加测试
4. 确保所有测试通过：`cd build && ctest`
5. 提交代码：
   - 遵循现有的代码风格
   - Commit message 清晰描述改动
6. 推送分支并发起 Pull Request

## 开发指南

### 代码风格

- 使用 **C++17** 标准
- 遵循项目现有代码风格
- 命名规范：
  - 类/结构体：`PascalCase`（如 `RaftNode`）
  - 函数/方法：`camelCase`（如 `AppendEntries`）
  - 变量：`snake_case`（如 `current_term`）
  - 常量：`kPascalCase`（如 `kNoTerm`）
- 每个头文件使用 `#pragma once` 防止重复包含
- 注释使用中文（与项目现有注释一致）

### 测试

- 所有新功能应有对应的单元测试
- C++ 测试文件放在 `tests/cpp/` 目录下
- Python 集成测试放在 `tests/` 目录下
- 运行测试：
  ```bash
  cd build && cmake .. && cmake --build . && ctest --output-on-failure
  ```

### 模块结构

```
src/
├── raft/          # Raft 共识算法
├── wal/           # 预写日志
├── snapshot/      # 快照管理
├── storage/       # MVCC 存储引擎
├── lease/         # 租约管理
├── watch/         # Watch 监听
├── server/        # 服务端整合
└── common/        # 公共类型定义
```

## 提交 Pull Request  Checklist

- [ ] 代码已编译通过
- [ ] 所有已有测试通过
- [ ] 新增功能包含单元测试
- [ ] 代码风格符合项目规范
- [ ] Commit message 有意义
- [ ] 已更新相关文档（如 API 变更）

## 问题反馈

任何问题请通过 GitHub Issues 反馈。
