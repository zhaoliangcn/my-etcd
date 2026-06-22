#pragma once

#include "common/types.h"
#include <string>
#include <fstream>
#include <vector>
#include <mutex>
#include <cstring>

namespace myetcd {

// WAL 记录类型
enum class WalRecordType : uint8_t {
    Entry = 0,
    HardState = 1,
    Snapshot = 2,
    Crc32 = 3,
};

// WAL (Write-Ahead Log) - 持久化日志
class WAL {
public:
    explicit WAL(const std::string& dir);
    ~WAL();

    // 禁止拷贝
    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    // 打开 WAL
    bool Open();

    // 关闭 WAL
    void Close();

    // 保存硬状态
    bool SaveHardState(const RaftHardState& state);

    // 读取硬状态
    bool LoadHardState(RaftHardState& state);

    // 追加日志条目
    bool AppendEntries(const std::vector<RaftEntry>& entries);

    // 读取所有日志条目
    std::vector<RaftEntry> ReadAllEntries();

    // 保存快照
    bool SaveSnapshot(const Snapshot& snapshot);

    // 读取快照
    bool LoadSnapshot(Snapshot& snapshot);

    // 截断日志 (从指定索引开始)
    bool TruncateFrom(Index idx);

    // 获取最后一条日志索引
    Index LastIndex() const;

    // 同步到磁盘
    bool Sync();

private:
    std::string DataDir() const { return dir_; }
    std::string WalDir() const { return dir_ + "/wal"; }
    std::string SnapDir() const { return dir_ + "/snap"; }

    bool EnsureDir(const std::string& path);
    std::string WalFilePath(int seq) const;

    // 序列化/反序列化
    std::vector<uint8_t> SerializeEntry(const RaftEntry& entry);
    RaftEntry DeserializeEntry(const uint8_t* data, size_t& offset);

    std::string dir_;
    std::fstream wal_file_;
    int wal_seq_ = 0;
    mutable std::mutex mu_;
    Index last_index_ = 0;
};

} // namespace myetcd