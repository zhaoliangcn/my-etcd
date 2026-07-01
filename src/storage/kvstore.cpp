#include "storage/kvstore.h"
#include <cstring>
#include <iostream>
#include <atomic>

namespace myetcd {

KVStore::KVStore(const std::string& data_dir)
    : backend_(data_dir)
{
}

KVStore::~KVStore() {
    Close();
}

bool KVStore::Open() {
    std::lock_guard<std::mutex> lock(mu_);
    return backend_.Open();
}

void KVStore::Close() {
    std::lock_guard<std::mutex> lock(mu_);
    backend_.Close();
}

Revision KVStore::Put(const std::string& key, const std::string& value, LeaseId lease_id) {
    std::lock_guard<std::mutex> lock(mu_);

    Revision rev = mvcc_.AllocateRevision();
    mvcc_.Put(key, rev, value, lease_id);
    backend_.Put(key, value);

    return rev;
}

PutWithPrevResult KVStore::PutWithPrev(const std::string& key, const std::string& value, LeaseId lease_id) {
    PutWithPrevResult result;
    std::lock_guard<std::mutex> lock(mu_);

    // 读取旧值（在写入之前，同一把锁保护）
    result.prev_kv = mvcc_.Get(key, 0);
    if (result.prev_kv) {
        auto val = backend_.Get(key);
        if (val) result.prev_kv->value = *val;
    }

    // 写入新值
    result.rev = mvcc_.AllocateRevision();
    mvcc_.Put(key, result.rev, value, lease_id);
    backend_.Put(key, value);

    // 读取新值
    result.new_kv = mvcc_.Get(key, 0);
    if (result.new_kv) {
        result.new_kv->value = value;
    }

    return result;
}

Revision KVStore::Delete(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);

    Revision rev = mvcc_.AllocateRevision();
    mvcc_.Delete(key, rev);
    backend_.Delete(key);

    return rev;
}

std::optional<KeyValue> KVStore::Get(const std::string& key, Revision at_rev) {
    std::lock_guard<std::mutex> lock(mu_);

    auto kv = mvcc_.Get(key, at_rev);
    if (!kv) return std::nullopt;

    // 最新版本（at_rev==0）：value 来自 Backend（最准确）
    // 历史版本（at_rev!=0）：value 来自 MVCC 版本记录
    if (at_rev == 0) {
        auto val = backend_.Get(key);
        if (val) {
            kv->value = *val;
        }
    }

    return kv;
}

std::vector<KeyValue> KVStore::Range(const std::string& start, const std::string& end,
                                       Revision at_rev, int64_t limit) {
    std::lock_guard<std::mutex> lock(mu_);

    auto kvs = mvcc_.Range(start, end, at_rev, limit);
    for (auto& kv : kvs) {
        auto val = backend_.Get(kv.key);
        if (val) {
            kv.value = *val;
        }
    }
    return kvs;
}

std::vector<KeyValue> KVStore::PrefixRange(const std::string& prefix,
                                             Revision at_rev, int64_t limit) {
    std::lock_guard<std::mutex> lock(mu_);

    auto kvs = mvcc_.PrefixRange(prefix, at_rev, limit);
    for (auto& kv : kvs) {
        auto val = backend_.Get(kv.key);
        if (val) {
            kv.value = *val;
        }
    }
    return kvs;
}

std::vector<std::string> KVStore::Keys() {
    std::lock_guard<std::mutex> lock(mu_);
    return mvcc_.Keys();
}

void KVStore::Compact(Revision rev) {
    std::lock_guard<std::mutex> lock(mu_);
    mvcc_.Compact(rev);
}

void KVStore::RestoreFromEntries(const std::vector<RaftEntry>& entries) {
    std::lock_guard<std::mutex> lock(mu_);

    for (const auto& entry : entries) {
        if (entry.type == EventType::PUT) {
            mvcc_.Put(entry.key, entry.index, entry.value, entry.lease_id);
            backend_.Put(entry.key, entry.value);
        } else if (entry.type == EventType::DELETE) {
            mvcc_.Delete(entry.key, entry.index);
            backend_.Delete(entry.key);
        }
    }
}

std::vector<uint8_t> KVStore::Serialize() {
    std::lock_guard<std::mutex> lock(mu_);

    std::vector<uint8_t> data;
    auto write_u32 = [&](uint32_t v) {
        data.push_back(static_cast<uint8_t>(v));
        data.push_back(static_cast<uint8_t>(v >> 8));
        data.push_back(static_cast<uint8_t>(v >> 16));
        data.push_back(static_cast<uint8_t>(v >> 24));
    };

    auto write_str = [&](const std::string& s) {
        write_u32(static_cast<uint32_t>(s.size()));
        data.insert(data.end(), s.begin(), s.end());
    };

    auto keys = backend_.Keys();
    write_u32(static_cast<uint32_t>(keys.size()));

    for (const auto& key : keys) {
        write_str(key);
        auto val = backend_.Get(key);
        write_str(val.value_or(""));
    }

    return data;
}

bool KVStore::Deserialize(const std::vector<uint8_t>& data) {
    std::lock_guard<std::mutex> lock(mu_);

    size_t offset = 0;
    if (offset + 4 > data.size()) return false;

    auto read_u32 = [&]() -> uint32_t {
        uint32_t v = 0;
        v |= static_cast<uint32_t>(data[offset++]);
        v |= static_cast<uint32_t>(data[offset++]) << 8;
        v |= static_cast<uint32_t>(data[offset++]) << 16;
        v |= static_cast<uint32_t>(data[offset++]) << 24;
        return v;
    };

    auto read_str = [&]() -> std::string {
        uint32_t len = read_u32();
        if (offset + len > data.size()) return "";
        std::string s(reinterpret_cast<const char*>(data.data() + offset), len);
        offset += len;
        return s;
    };

    uint32_t count = read_u32();
    std::map<std::string, std::string> kvs;
    for (uint32_t i = 0; i < count; ++i) {
        if (offset >= data.size()) break;
        std::string key = read_str();
        std::string value = read_str();
        kvs[key] = value;
    }

    // 清空并重建后端存储
    backend_.BatchPut(kvs);

    // 重建 MVCC 索引（每个 key 创建一个索引条目）
    for (const auto& [key, value] : kvs) {
        Revision rev = mvcc_.AllocateRevision();
        mvcc_.Put(key, rev, value, 0);
    }

    return true;
}

} // namespace myetcd
