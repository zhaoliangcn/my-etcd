#pragma once

#include "common/types.h"
#include <vector>
#include <string>
#include <mutex>
#include <functional>

namespace myetcd {

// 快照管理器 - 负责日志压缩
class SnapshotManager {
public:
    explicit SnapshotManager(const std::string& snap_dir, int64_t max_snapshots = 5);
    ~SnapshotManager() = default;

    // 创建快照
    bool CreateSnapshot(Index last_index, Term last_term, const std::vector<uint8_t>& data);

    // 加载最新快照
    bool LoadLatestSnapshot(Snapshot& snapshot);

    // 清理旧快照
    void CleanupOldSnapshots();

    // 获取快照目录
    std::string SnapDir() const { return snap_dir_; }

    // 检查是否需要创建快照 (日志条目数超过阈值)
    bool ShouldSnapshot(Index last_applied, Index snapshot_index, int64_t threshold = 10000) const;

private:
    std::string snap_dir_;
    int64_t max_snapshots_;
    mutable std::mutex mu_;
};

} // namespace myetcd