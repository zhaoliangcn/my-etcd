#pragma once

#include "common/types.h"
#include "storage/mvcc.h"
#include "storage/backend.h"
#include <memory>
#include <mutex>
#include <shared_mutex>

namespace myetcd {

// KVStore - 协调 MVCC 索引和后端持久化存储
class KVStore {
public:
    explicit KVStore(const std::string& data_dir);
    ~KVStore();

    KVStore(const KVStore&) = delete;
    KVStore& operator=(const KVStore&) = delete;

    // 打开存储
    bool Open();

    // 关闭存储
    void Close();

    // 写入
    Revision Put(const std::string& key, const std::string& value, LeaseId lease_id = 0);

    // 删除
    Revision Delete(const std::string& key);

    // 读取
    std::optional<KeyValue> Get(const std::string& key, Revision at_rev = 0);

    // 范围查询
    std::vector<KeyValue> Range(const std::string& start, const std::string& end,
                                 Revision at_rev = 0, int64_t limit = 0);

    // 前缀查询
    std::vector<KeyValue> PrefixRange(const std::string& prefix,
                                       Revision at_rev = 0, int64_t limit = 0);

    // 获取所有 key
    std::vector<std::string> Keys();

    // 压缩
    void Compact(Revision rev);

    // 从 Raft 日志条目恢复
    void RestoreFromEntries(const std::vector<RaftEntry>& entries);

    // 当前 revision
    Revision CurrentRevision() const { return mvcc_.CurrentRevision(); }

    // 分配 revision
    Revision AllocateRevision() { return mvcc_.AllocateRevision(); }

    // 获取底层 MVCC 索引
    MVCC& GetMVCC() { return mvcc_; }
    Backend& GetBackend() { return backend_; }

    // 序列化所有数据 (用于快照)
    std::vector<uint8_t> Serialize();

    // 从序列化数据恢复 (用于快照)
    bool Deserialize(const std::vector<uint8_t>& data);

private:
    MVCC mvcc_;
    Backend backend_;
    std::mutex mu_;
};

} // namespace myetcd