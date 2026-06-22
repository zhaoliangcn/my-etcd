#include "storage/mvcc.h"
#include <algorithm>

namespace myetcd {

// KeyIndex 实现

std::optional<MvccVersion> KeyIndex::Get(Revision rev) const {
    for (const auto& v : versions) {
        if (v.revision == rev) return v;
    }
    return std::nullopt;
}

std::optional<MvccVersion> KeyIndex::Latest() const {
    if (versions.empty()) return std::nullopt;
    return versions.back();
}

void KeyIndex::Put(Revision rev, LeaseId lease_id) {
    MvccVersion ver;
    ver.revision = rev;
    ver.lease_id = lease_id;
    ver.tombstone = false;

    if (versions.empty()) {
        ver.version = 1;
        ver.create_revision = rev;
    } else {
        ver.version = versions.back().version + 1;
        ver.create_revision = versions[0].create_revision;
    }

    versions.push_back(ver);
    last_revision = rev;
}

void KeyIndex::Tombstone(Revision rev) {
    MvccVersion ver;
    ver.revision = rev;
    ver.tombstone = true;

    if (!versions.empty()) {
        ver.version = versions.back().version + 1;
        ver.create_revision = versions[0].create_revision;
    } else {
        ver.version = 1;
        ver.create_revision = rev;
    }

    versions.push_back(ver);
    last_revision = rev;
}

bool KeyIndex::IsDeleted() const {
    if (versions.empty()) return true;
    return versions.back().tombstone;
}

void KeyIndex::Compact(Revision rev) {
    auto it = std::remove_if(versions.begin(), versions.end(),
        [rev](const MvccVersion& v) {
            return v.revision < rev && !v.tombstone;
        });
    if (it != versions.begin()) {
        versions.erase(versions.begin(), it);
    }
}

// MVCC 实现

MVCC::MVCC() {}

void MVCC::Put(const std::string& key, Revision rev, LeaseId lease_id) {
    std::unique_lock<std::shared_mutex> lock(mu_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        KeyIndex ki;
        ki.key = key;
        ki.Put(rev, lease_id);
        index_[key] = std::move(ki);
    } else {
        it->second.Put(rev, lease_id);
    }
}

void MVCC::Delete(const std::string& key, Revision rev) {
    std::unique_lock<std::shared_mutex> lock(mu_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        KeyIndex ki;
        ki.key = key;
        ki.Tombstone(rev);
        index_[key] = std::move(ki);
    } else {
        it->second.Tombstone(rev);
    }
}

std::optional<KeyValue> MVCC::Get(const std::string& key, Revision at_rev) {
    std::shared_lock<std::shared_mutex> lock(mu_);

    auto it = index_.find(key);
    if (it == index_.end()) return std::nullopt;

    const auto& ki = it->second;
    if (ki.IsDeleted()) return std::nullopt;

    const MvccVersion* ver = nullptr;
    if (at_rev == 0) {
        auto latest = ki.Latest();
        if (!latest || latest->tombstone) return std::nullopt;
        ver = &ki.versions.back(); // 需要重新获取引用
    } else {
        // 找到 at_rev 之前的最新版本
        for (auto it2 = ki.versions.rbegin(); it2 != ki.versions.rend(); ++it2) {
            if (it2->revision <= at_rev && !it2->tombstone) {
                // 需要返回副本
                auto opt = ki.Get(it2->revision);
                if (opt && !opt->tombstone) {
                    ver = &ki.versions[ki.versions.size() - 1 - (it2 - ki.versions.rbegin())];
                }
                break;
            }
        }
    }

    if (!ver || ver->tombstone) return std::nullopt;

    KeyValue kv;
    kv.key = key;
    kv.create_revision = ver->create_revision;
    kv.mod_revision = ver->revision;
    kv.version = ver->version;
    kv.lease_id = ver->lease_id;
    return kv;
}

std::vector<KeyValue> MVCC::Range(const std::string& start, const std::string& end,
                                    Revision at_rev, int64_t limit) {
    std::shared_lock<std::shared_mutex> lock(mu_);
    std::vector<KeyValue> result;

    auto it = index_.lower_bound(start);
    for (; it != index_.end() && it->first < end; ++it) {
        if (limit > 0 && static_cast<int64_t>(result.size()) >= limit) break;
        if (it->second.IsDeleted()) continue;

        const MvccVersion* ver = nullptr;
        if (at_rev == 0) {
            auto latest = it->second.Latest();
            if (!latest || latest->tombstone) continue;
            ver = &it->second.versions.back();
        } else {
            for (auto rit = it->second.versions.rbegin(); rit != it->second.versions.rend(); ++rit) {
                if (rit->revision <= at_rev && !rit->tombstone) {
                    ver = &(*rit);
                    break;
                }
            }
        }

        if (!ver || ver->tombstone) continue;

        KeyValue kv;
        kv.key = it->first;
        kv.create_revision = ver->create_revision;
        kv.mod_revision = ver->revision;
        kv.version = ver->version;
        kv.lease_id = ver->lease_id;
        result.push_back(kv);
    }

    return result;
}

std::vector<KeyValue> MVCC::PrefixRange(const std::string& prefix,
                                          Revision at_rev, int64_t limit) {
    std::shared_lock<std::shared_mutex> lock(mu_);
    std::vector<KeyValue> result;

    auto it = index_.lower_bound(prefix);
    for (; it != index_.end(); ++it) {
        if (it->first.compare(0, prefix.size(), prefix) != 0) break;
        if (limit > 0 && static_cast<int64_t>(result.size()) >= limit) break;
        if (it->second.IsDeleted()) continue;

        const MvccVersion* ver = nullptr;
        if (at_rev == 0) {
            auto latest = it->second.Latest();
            if (!latest || latest->tombstone) continue;
            ver = &it->second.versions.back();
        } else {
            for (auto rit = it->second.versions.rbegin(); rit != it->second.versions.rend(); ++rit) {
                if (rit->revision <= at_rev && !rit->tombstone) {
                    ver = &(*rit);
                    break;
                }
            }
        }

        if (!ver || ver->tombstone) continue;

        KeyValue kv;
        kv.key = it->first;
        kv.create_revision = ver->create_revision;
        kv.mod_revision = ver->revision;
        kv.version = ver->version;
        kv.lease_id = ver->lease_id;
        result.push_back(kv);
    }

    return result;
}

std::vector<std::string> MVCC::Keys() {
    std::shared_lock<std::shared_mutex> lock(mu_);
    std::vector<std::string> keys;
    keys.reserve(index_.size());
    for (const auto& [key, ki] : index_) {
        if (!ki.IsDeleted()) {
            keys.push_back(key);
        }
    }
    return keys;
}

void MVCC::Compact(Revision rev) {
    std::unique_lock<std::shared_mutex> lock(mu_);

    auto it = index_.begin();
    while (it != index_.end()) {
        it->second.Compact(rev);
        // 如果所有版本都被压缩且已删除，删除这个 key
        if (it->second.versions.empty() || 
            (it->second.IsDeleted() && it->second.last_revision <= rev)) {
            it = index_.erase(it);
        } else {
            ++it;
        }
    }
}

Revision MVCC::AllocateRevision() {
    return current_rev_.fetch_add(1);
}

std::optional<KeyIndex> MVCC::GetKeyIndex(const std::string& key) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    auto it = index_.find(key);
    if (it != index_.end()) {
        return it->second;
    }
    return std::nullopt;
}

} // namespace myetcd