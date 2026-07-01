#pragma once

#include "common/types.h"
#include <map>
#include <vector>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <atomic>
namespace myetcd {

// MVCC 版本信息
struct MvccVersion {
    Revision revision;
    int64_t version; // 该 key 被修改的次数
    Revision create_revision;
    bool tombstone; // 是否已删除
    LeaseId lease_id;
    std::string value; // 该版本对应的 value
};

// Key 索引 - 跟踪一个 key 的所有版本
struct KeyIndex {
    std::string key;
    std::vector<MvccVersion> versions;
    Revision last_revision = 0;

    // 获取指定 revision 的版本
    std::optional<MvccVersion> Get(Revision rev) const;

    // 获取最新版本
    std::optional<MvccVersion> Latest() const;

    // 添加新版本
    void Put(Revision rev, const std::string& value, LeaseId lease_id);

    // 标记删除
    void Tombstone(Revision rev);

    // 是否已删除
    bool IsDeleted() const;

    // 压缩：删除指定 revision 之前的版本
    void Compact(Revision rev);
};

// MVCC 存储 - 多版本并发控制
class MVCC {
public:
    MVCC();
    ~MVCC() = default;

    // 写入
    void Put(const std::string& key, Revision rev, const std::string& value, LeaseId lease_id = 0);

    // 删除
    void Delete(const std::string& key, Revision rev);

    // 读取指定 revision
    std::optional<KeyValue> Get(const std::string& key, Revision at_rev = 0);

    // 范围查询
    std::vector<KeyValue> Range(const std::string& start, const std::string& end,
                                 Revision at_rev = 0, int64_t limit = 0);

    // 前缀查询
    std::vector<KeyValue> PrefixRange(const std::string& prefix,
                                       Revision at_rev = 0, int64_t limit = 0);

    // 获取所有 key
    std::vector<std::string> Keys();

    // 压缩：删除指定 revision 之前的版本
    void Compact(Revision rev);

    // 获取当前 revision
    Revision CurrentRevision() const { return current_rev_.load(); }

    // 分配新 revision
    Revision AllocateRevision();

    // 获取 KeyIndex（用于调试）
    std::optional<KeyIndex> GetKeyIndex(const std::string& key) const;

private:
    mutable std::shared_mutex mu_;
    std::map<std::string, KeyIndex> index_;
    std::atomic<Revision> current_rev_{1};
};

} // namespace myetcd
