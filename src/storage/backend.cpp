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

    // 获取文件大小
    ifs.seekg(0, std::ios::end);
    size_t file_size = ifs.tellg();
    ifs.seekg(0, std::ios::beg);

    // 验证 magic header
    if (file_size < 8) return true; // 太小，视为旧格式或空文件
    uint32_t magic = 0, version = 0;
    ifs.read(reinterpret_cast<char*>(&magic), sizeof(uint32_t));
    ifs.read(reinterpret_cast<char*>(&version), sizeof(uint32_t));
    if (magic != kMagic) {
        std::cerr << "[Backend] Invalid magic number, treating as empty" << std::endl;
        return true;
    }
    if (version > kVersion) {
        std::cerr << "[Backend] Unknown version " << version << std::endl;
        return true;
    }

    constexpr size_t kMaxKeyLen = 1024 * 1024;    // 1MB
    constexpr size_t kMaxValueLen = 64 * 1024 * 1024; // 64MB

    size_t bytes_read = 8; // 已读取 magic + version
    while (bytes_read + 4 <= file_size) {
        uint32_t key_len = 0;
        ifs.read(reinterpret_cast<char*>(&key_len), sizeof(uint32_t));
        if (!ifs.gcount()) break;
        bytes_read += ifs.gcount();

        if (key_len == 0 || key_len > kMaxKeyLen || bytes_read + key_len > file_size) break;

        std::string key(key_len, '\0');
        ifs.read(&key[0], key_len);
        if (static_cast<size_t>(ifs.gcount()) != key_len) break;
        bytes_read += key_len;

        if (bytes_read + 4 > file_size) break;
        uint32_t value_len = 0;
        ifs.read(reinterpret_cast<char*>(&value_len), sizeof(uint32_t));
        if (!ifs.gcount()) break;
        bytes_read += ifs.gcount();

        if (value_len > kMaxValueLen || bytes_read + value_len > file_size) break;

        std::string value(value_len, '\0');
        ifs.read(&value[0], value_len);
        if (static_cast<size_t>(ifs.gcount()) != value_len) break;
        bytes_read += value_len;

        store_[key] = value;
    }

    return true;
}

bool Backend::FlushToDisk() {
    std::string path = DataFilePath();
    std::string tmp_path = path + ".tmp";

    // 写临时文件，确保数据完整后再原子重命名
    std::ofstream ofs(tmp_path, std::ios::binary | std::ios::trunc);
    if (!ofs) return false;

    // 写入 magic header
    uint32_t magic = kMagic;
    uint32_t version = kVersion;
    ofs.write(reinterpret_cast<const char*>(&magic), sizeof(uint32_t));
    ofs.write(reinterpret_cast<const char*>(&version), sizeof(uint32_t));

    for (const auto& [key, value] : store_) {
        uint32_t key_len = static_cast<uint32_t>(key.size());
        ofs.write(reinterpret_cast<const char*>(&key_len), sizeof(uint32_t));
        ofs.write(key.data(), key_len);

        uint32_t value_len = static_cast<uint32_t>(value.size());
        ofs.write(reinterpret_cast<const char*>(&value_len), sizeof(uint32_t));
        ofs.write(value.data(), value_len);
    }

    ofs.flush();
    if (!ofs) {
        std::error_code ec;
        fs::remove(tmp_path, ec);
        return false;
    }
    ofs.close();

    std::error_code ec;
    fs::rename(tmp_path, path, ec);
    if (ec) {
        std::error_code ec2;
        fs::remove(tmp_path, ec2);
        return false;
    }
    return true;
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