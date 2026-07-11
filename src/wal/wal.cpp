#include "wal/wal.h"
#include <filesystem>
#include <iostream>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

namespace myetcd {

namespace fs = std::filesystem;

WAL::WAL(const std::string& dir) : dir_(dir) {}

WAL::~WAL() {
    Close();
}

bool WAL::EnsureDir(const std::string& path) {
    std::error_code ec;
    if (!fs::exists(path, ec)) {
        fs::create_directories(path, ec);
    }
    return !ec;
}

std::string WAL::WalFilePath(int seq) const {
    return WalDir() + "/wal_" + std::to_string(seq) + ".log";
}

bool WAL::Open() {
    std::lock_guard<std::mutex> lock(mu_);

    if (!EnsureDir(DataDir())) return false;
    if (!EnsureDir(WalDir())) return false;
    if (!EnsureDir(SnapDir())) return false;

    // 找到最大的 WAL 文件序号
    wal_seq_ = 0;
    std::error_code ec;
    for (auto& entry : fs::directory_iterator(WalDir(), ec)) {
        if (ec) break;
        std::string name = entry.path().filename().string();
        if (name.find("wal_") == 0) {
            try {
                int seq = std::stoi(name.substr(4, name.size() - 8));
                if (seq >= wal_seq_) wal_seq_ = seq;
            } catch (const std::invalid_argument&) {
                // 文件名解析失败，跳过
            } catch (const std::out_of_range&) {
                // 序号溢出，跳过
            }
        }
    }

    // 打开或创建 WAL 文件
    wal_file_.open(WalFilePath(wal_seq_), std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!wal_file_.is_open()) {
        wal_file_.open(WalFilePath(wal_seq_), std::ios::out | std::ios::binary);
        if (!wal_file_.is_open()) return false;
        wal_file_.close();
        wal_file_.open(WalFilePath(wal_seq_), std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    }

    // 打开 fd 用于 fsync
    wal_fd_ = ::open(WalFilePath(wal_seq_).c_str(), O_RDWR);
    if (wal_fd_ < 0) {
        std::cerr << "[WAL] Warning: cannot open fd for fsync" << std::endl;
    }

    return wal_file_.is_open();
}

void WAL::Close() {
    std::lock_guard<std::mutex> lock(mu_);
    if (wal_file_.is_open()) {
        wal_file_.flush();
        wal_file_.close();
    }
    if (wal_fd_ >= 0) {
        ::fsync(wal_fd_);
        ::close(wal_fd_);
        wal_fd_ = -1;
    }
}

bool WAL::SaveHardState(const RaftHardState& state) {
    std::lock_guard<std::mutex> lock(mu_);
    std::string path = DataDir() + "/hardstate";
    std::string tmp_path = path + ".tmp";

    // 写临时文件，再原子重命名
    std::ofstream ofs(tmp_path, std::ios::binary | std::ios::trunc);
    if (!ofs) return false;

    ofs.write(reinterpret_cast<const char*>(&state.current_term), sizeof(Term));
    NodeId voted = state.voted_for.value_or(kNoNodeId);
    ofs.write(reinterpret_cast<const char*>(&voted), sizeof(NodeId));
    ofs.write(reinterpret_cast<const char*>(&state.commit_index), sizeof(Index));

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

bool WAL::LoadHardState(RaftHardState& state) {
    std::lock_guard<std::mutex> lock(mu_);
    std::string path = DataDir() + "/hardstate";
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) return false;

    ifs.read(reinterpret_cast<char*>(&state.current_term), sizeof(Term));
    NodeId voted;
    ifs.read(reinterpret_cast<char*>(&voted), sizeof(NodeId));
    state.voted_for = (voted == kNoNodeId) ? std::nullopt : std::optional<NodeId>(voted);
    ifs.read(reinterpret_cast<char*>(&state.commit_index), sizeof(Index));

    return ifs.good();
}

std::vector<uint8_t> WAL::SerializeEntry(const RaftEntry& entry) {
    std::vector<uint8_t> data;
    data.reserve(256);

    auto write_u64 = [&](uint64_t v) {
        data.push_back(static_cast<uint8_t>(v));
        data.push_back(static_cast<uint8_t>(v >> 8));
        data.push_back(static_cast<uint8_t>(v >> 16));
        data.push_back(static_cast<uint8_t>(v >> 24));
        data.push_back(static_cast<uint8_t>(v >> 32));
        data.push_back(static_cast<uint8_t>(v >> 40));
        data.push_back(static_cast<uint8_t>(v >> 48));
        data.push_back(static_cast<uint8_t>(v >> 56));
    };

    auto write_u32 = [&](uint32_t v) {
        data.push_back(static_cast<uint8_t>(v));
        data.push_back(static_cast<uint8_t>(v >> 8));
        data.push_back(static_cast<uint8_t>(v >> 16));
        data.push_back(static_cast<uint8_t>(v >> 24));
    };

    auto write_u8 = [&](uint8_t v) {
        data.push_back(v);
    };

    auto write_str = [&](const std::string& s) {
        write_u32(static_cast<uint32_t>(s.size()));
        data.insert(data.end(), s.begin(), s.end());
    };

    write_u8(static_cast<uint8_t>(WalRecordType::Entry));
    write_u64(entry.term);
    write_u64(entry.index);
    write_u8(static_cast<uint8_t>(entry.type));
    write_str(entry.key);
    write_str(entry.value);
    write_u64(static_cast<uint64_t>(entry.lease_id));

    // 写入 ConfChange 数据（仅 CONF_CHANGE 类型时）
    if (entry.type == EventType::CONF_CHANGE) {
        write_u8(static_cast<uint8_t>(entry.conf_change.type));
        write_u64(entry.conf_change.node_id);
        write_str(entry.conf_change.peer_addr);
    }

    // 写入总长度前缀
    uint32_t total_len = static_cast<uint32_t>(data.size());
    data.insert(data.begin(), {
        static_cast<uint8_t>(total_len),
        static_cast<uint8_t>(total_len >> 8),
        static_cast<uint8_t>(total_len >> 16),
        static_cast<uint8_t>(total_len >> 24)});

    return data;
}

RaftEntry WAL::DeserializeEntry(const uint8_t* data, size_t size, size_t& offset) {
    RaftEntry entry;

    auto check_bounds = [&](size_t n) -> bool {
        return offset + n <= size;
    };

    auto read_u64 = [&]() -> uint64_t {
        uint64_t v = 0;
        v |= static_cast<uint64_t>(data[offset++]);
        v |= static_cast<uint64_t>(data[offset++]) << 8;
        v |= static_cast<uint64_t>(data[offset++]) << 16;
        v |= static_cast<uint64_t>(data[offset++]) << 24;
        v |= static_cast<uint64_t>(data[offset++]) << 32;
        v |= static_cast<uint64_t>(data[offset++]) << 40;
        v |= static_cast<uint64_t>(data[offset++]) << 48;
        v |= static_cast<uint64_t>(data[offset++]) << 56;
        return v;
    };

    auto read_u32 = [&]() -> uint32_t {
        uint32_t v = 0;
        v |= static_cast<uint32_t>(data[offset++]);
        v |= static_cast<uint32_t>(data[offset++]) << 8;
        v |= static_cast<uint32_t>(data[offset++]) << 16;
        v |= static_cast<uint32_t>(data[offset++]) << 24;
        return v;
    };

    auto read_u8 = [&]() -> uint8_t {
        return data[offset++];
    };

    auto read_str = [&]() -> std::string {
        uint32_t len = read_u32();
        if (!check_bounds(len)) { offset = size; return ""; }
        std::string s(reinterpret_cast<const char*>(data + offset), len);
        offset += len;
        return s;
    };

    // 跳过长度前缀
    if (!check_bounds(4)) return entry;
    read_u32();

    if (!check_bounds(1)) return entry;
    uint8_t type = read_u8();
    if (static_cast<WalRecordType>(type) != WalRecordType::Entry) {
        return entry;
    }

    if (!check_bounds(8 + 8 + 1)) return entry;
    entry.term = read_u64();
    entry.index = read_u64();
    entry.type = static_cast<EventType>(read_u8());
    entry.key = read_str();
    entry.value = read_str();
    if (!check_bounds(8)) return entry;
    entry.lease_id = static_cast<LeaseId>(read_u64());

    // 读取 ConfChange 数据（仅 CONF_CHANGE 类型）
    if (entry.type == EventType::CONF_CHANGE) {
        if (!check_bounds(1 + 8)) return entry;
        entry.conf_change.type = static_cast<ConfChangeType>(read_u8());
        entry.conf_change.node_id = read_u64();
        entry.conf_change.peer_addr = read_str();
    }

    return entry;
}

bool WAL::AppendEntries(const std::vector<RaftEntry>& entries) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!wal_file_.is_open()) return false;

    for (const auto& entry : entries) {
        auto data = SerializeEntry(entry);
        wal_file_.write(reinterpret_cast<const char*>(data.data()), data.size());
        last_index_ = entry.index;
    }

    wal_file_.flush();
    if (wal_fd_ >= 0) {
        ::fsync(wal_fd_);
    }
    return wal_file_.good();
}

std::vector<RaftEntry> WAL::ReadAllEntries() {
    std::lock_guard<std::mutex> lock(mu_);
    return ReadAllEntriesUnlocked();
}

std::vector<RaftEntry> WAL::ReadAllEntriesUnlocked() {
    // 调用者必须已持有 mu_
    std::vector<RaftEntry> entries;

    // 读取所有 WAL 文件
    for (int seq = 0; seq <= wal_seq_; ++seq) {
        std::ifstream ifs(WalFilePath(seq), std::ios::binary);
        if (!ifs) continue;

        // 获取文件大小
        ifs.seekg(0, std::ios::end);
        size_t file_size = ifs.tellg();
        ifs.seekg(0, std::ios::beg);

        std::vector<uint8_t> buffer(file_size);
        ifs.read(reinterpret_cast<char*>(buffer.data()), file_size);
        size_t actual_read = static_cast<size_t>(ifs.gcount());
        if (actual_read != file_size) {
            std::cerr << "[WAL] Partial read of WAL file " << seq
                      << ": expected " << file_size << " got " << actual_read << std::endl;
            file_size = actual_read; // 使用实际读取的大小
        }

        size_t offset = 0;
        while (offset + 4 <= file_size) {
            // 读取长度前缀
            uint32_t len = 0;
            len |= static_cast<uint32_t>(buffer[offset]);
            len |= static_cast<uint32_t>(buffer[offset + 1]) << 8;
            len |= static_cast<uint32_t>(buffer[offset + 2]) << 16;
            len |= static_cast<uint32_t>(buffer[offset + 3]) << 24;

            if (len == 0 || offset + 4 + len > file_size) break;

            size_t entry_offset = offset;
            RaftEntry entry = DeserializeEntry(buffer.data(), file_size, entry_offset);
            if (entry.index > 0) {
                entries.push_back(entry);
                last_index_ = std::max(last_index_, entry.index);
            }
            offset += 4 + len;
        }
    }

    return entries;
}

bool WAL::SaveSnapshot(const Snapshot& snapshot) {
    std::lock_guard<std::mutex> lock(mu_);
    std::string path = SnapDir() + "/snapshot_" + std::to_string(snapshot.last_index);
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    if (!ofs) return false;

    ofs.write(reinterpret_cast<const char*>(&snapshot.last_index), sizeof(Index));
    ofs.write(reinterpret_cast<const char*>(&snapshot.last_term), sizeof(Term));

    uint32_t data_size = static_cast<uint32_t>(snapshot.data.size());
    ofs.write(reinterpret_cast<const char*>(&data_size), sizeof(uint32_t));
    if (!snapshot.data.empty()) {
        ofs.write(reinterpret_cast<const char*>(snapshot.data.data()), snapshot.data.size());
    }

    return ofs.good();
}

bool WAL::LoadSnapshot(Snapshot& snapshot) {
    std::lock_guard<std::mutex> lock(mu_);

    // 查找最新的快照
    std::string latest_snap;
    Index latest_idx = 0;
    std::error_code ec;
    for (auto& entry : fs::directory_iterator(SnapDir(), ec)) {
        if (ec) break;
        std::string name = entry.path().filename().string();
        if (name.find("snapshot_") == 0) {
            try {
                Index idx = std::stoull(name.substr(9));
                if (idx > latest_idx) {
                    latest_idx = idx;
                    latest_snap = entry.path().string();
                }
            } catch (const std::invalid_argument&) {
                // 快照文件名解析失败，跳过
            } catch (const std::out_of_range&) {
                // 索引溢出，跳过
            }
        }
    }

    if (latest_snap.empty()) return false;

    std::ifstream ifs(latest_snap, std::ios::binary);
    if (!ifs) return false;

    ifs.read(reinterpret_cast<char*>(&snapshot.last_index), sizeof(Index));
    ifs.read(reinterpret_cast<char*>(&snapshot.last_term), sizeof(Term));

    uint32_t data_size = 0;
    ifs.read(reinterpret_cast<char*>(&data_size), sizeof(uint32_t));
    snapshot.data.resize(data_size);
    if (data_size > 0) {
        ifs.read(reinterpret_cast<char*>(snapshot.data.data()), data_size);
    }

    return ifs.good();
}

bool WAL::TruncateFrom(Index idx) {
    std::lock_guard<std::mutex> lock(mu_);
    // 简化的截断实现：重建 WAL 文件
    wal_file_.close();
    if (wal_fd_ >= 0) {
        ::close(wal_fd_);
        wal_fd_ = -1;
    }

    auto entries = ReadAllEntriesUnlocked();
    std::vector<RaftEntry> kept;
    for (auto& e : entries) {
        if (e.index >= idx) {
            kept.push_back(e);
        }
    }

    // 删除旧 WAL 文件
    for (int seq = 0; seq < wal_seq_; ++seq) {
        std::error_code ec;
        fs::remove(WalFilePath(seq), ec);
    }

    // 写入新文件
    wal_seq_++;
    wal_file_.open(WalFilePath(wal_seq_), std::ios::out | std::ios::binary);
    if (!wal_file_.is_open()) return false;

    for (auto& e : kept) {
        auto data = SerializeEntry(e);
        wal_file_.write(reinterpret_cast<const char*>(data.data()), data.size());
    }

    wal_file_.flush();
    // 重新打开 fd 用于后续 fsync
    wal_fd_ = ::open(WalFilePath(wal_seq_).c_str(), O_RDWR);

    last_index_ = kept.empty() ? idx - 1 : kept.back().index;
    return true;
}

Index WAL::LastIndex() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_index_;
}

bool WAL::Sync() {
    std::lock_guard<std::mutex> lock(mu_);
    if (wal_file_.is_open()) {
        wal_file_.flush();
        if (wal_fd_ >= 0) {
            ::fsync(wal_fd_);
        }
        return wal_file_.good();
    }
    return false;
}

} // namespace myetcd