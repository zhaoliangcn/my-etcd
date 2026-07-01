#include "storage/backend.h"
#include <filesystem>
#include <iostream>
#include <cstring>

namespace myetcd {

namespace fs = std::filesystem;

Backend::Backend(const std::string& data_dir) : data_dir_(data_dir) {}

Backend::~Backend() {
    Close();
}

bool Backend::Open() {
    std::lock_guard<std::mutex> lock(mu_);

    std::error_code ec;
    if (!fs::exists(data_dir_, ec)) {
        fs::create_directories(data_dir_, ec);
    }

    return LoadFromDisk();
}

void Backend::Close() {
    std::lock_guard<std::mutex> lock(mu_);
    FlushToDisk();
}

bool Backend::LoadFromDisk() {
    std::ifstream ifs(DataFilePath(), std::ios::binary);
    if (!ifs) return true; // 文件不存在，首次启动

    while (ifs.good()) {
        uint32_t key_len = 0;
        ifs.read(reinterpret_cast<char*>(&key_len), sizeof(uint32_t));
        if (!ifs.good() || key_len == 0) break;

        std::string key(key_len, '\0');
        ifs.read(&key[0], key_len);
        if (!ifs.good()) break;

        uint32_t value_len = 0;
        ifs.read(reinterpret_cast<char*>(&value_len), sizeof(uint32_t));
        if (!ifs.good()) break;

        std::string value(value_len, '\0');
        ifs.read(&value[0], value_len);
        if (!ifs.good()) break;

        store_[key] = value;
    }

    return true;
}

bool Backend::FlushToDisk() {
    std::ofstream ofs(DataFilePath(), std::ios::binary | std::ios::trunc);
    if (!ofs) return false;

    for (const auto& [key, value] : store_) {
        uint32_t key_len = static_cast<uint32_t>(key.size());
        ofs.write(reinterpret_cast<const char*>(&key_len), sizeof(uint32_t));
        ofs.write(key.data(), key_len);

        uint32_t value_len = static_cast<uint32_t>(value.size());
        ofs.write(reinterpret_cast<const char*>(&value_len), sizeof(uint32_t));
        ofs.write(value.data(), value_len);
    }

    ofs.flush();
    return ofs.good();
}

bool Backend::Put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mu_);
    store_[key] = value;
    return true;
}

std::optional<std::string> Backend::Get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = store_.find(key);
    if (it != store_.end()) {
        return it->second;
    }
    return std::nullopt;
}

bool Backend::Delete(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    store_.erase(key);
    return true;
}

std::vector<KeyValue> Backend::Range(const std::string& start, const std::string& end, int64_t limit) {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<KeyValue> result;

    auto it = store_.lower_bound(start);
    for (; it != store_.end() && it->first < end; ++it) {
        if (limit > 0 && static_cast<int64_t>(result.size()) >= limit) break;
        KeyValue kv;
        kv.key = it->first;
        kv.value = it->second;
        result.push_back(kv);
    }

    return result;
}

std::vector<KeyValue> Backend::PrefixRange(const std::string& prefix, int64_t limit) {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<KeyValue> result;

    auto it = store_.lower_bound(prefix);
    for (; it != store_.end(); ++it) {
        if (it->first.compare(0, prefix.size(), prefix) != 0) break;
        if (limit > 0 && static_cast<int64_t>(result.size()) >= limit) break;
        KeyValue kv;
        kv.key = it->first;
        kv.value = it->second;
        result.push_back(kv);
    }

    return result;
}

bool Backend::BatchPut(const std::map<std::string, std::string>& kvs) {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& [key, value] : kvs) {
        store_[key] = value;
    }
    return true;
}

std::vector<std::string> Backend::Keys() {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::string> keys;
    keys.reserve(store_.size());
    for (const auto& [key, _] : store_) {
        keys.push_back(key);
    }
    return keys;
}

bool Backend::Sync() {
    std::lock_guard<std::mutex> lock(mu_);
    return FlushToDisk();
}

} // namespace myetcd