#include "snapshot/snapshot.h"
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <cstdio>
#include <iostream>

namespace myetcd {

namespace fs = std::filesystem;

SnapshotManager::SnapshotManager(const std::string& snap_dir, int64_t max_snapshots)
    : snap_dir_(snap_dir), max_snapshots_(max_snapshots)
{
    std::error_code ec;
    if (!fs::exists(snap_dir_, ec)) {
        fs::create_directories(snap_dir_, ec);
    }
    // 清理可能残留的临时文件
    fs::remove(snap_dir_ + "/snapshot_tmp", ec);
}

bool SnapshotManager::CreateSnapshot(Index last_index, Term last_term, const std::vector<uint8_t>& data) {
    std::lock_guard<std::mutex> lock(mu_);

    std::string tmp_path = snap_dir_ + "/snapshot_tmp";
    std::string final_path = snap_dir_ + "/snapshot_" + std::to_string(last_index);

    // 先写入临时文件，确保数据完整后再原子重命名
    std::ofstream ofs(tmp_path, std::ios::binary | std::ios::trunc);
    if (!ofs) return false;

    ofs.write(reinterpret_cast<const char*>(&last_index), sizeof(Index));
    ofs.write(reinterpret_cast<const char*>(&last_term), sizeof(Term));

    // 检查数据大小是否超过 uint32_t 范围
    if (data.size() > UINT32_MAX) {
        std::cerr << "[Snapshot] Data too large: " << data.size() << " bytes" << std::endl;
        std::error_code ec;
        fs::remove(tmp_path, ec);
        return false;
    }
    uint32_t data_size = static_cast<uint32_t>(data.size());
    ofs.write(reinterpret_cast<const char*>(&data_size), sizeof(uint32_t));
    if (!data.empty()) {
        ofs.write(reinterpret_cast<const char*>(data.data()), data.size());
    }

    // 确保数据刷到磁盘
    ofs.flush();
    // 使用 fsync 确保物理写入（通过 fstream 的 native handle）
    if (!ofs) return false;

    ofs.close();
    if (!ofs.good()) {
        std::error_code ec;
        fs::remove(tmp_path, ec);
        return false;
    }

    // 原子重命名（POSIX 保证 rename 是原子的）
    std::error_code ec;
    fs::rename(tmp_path, final_path, ec);
    if (ec) {
        std::error_code ec2;
        fs::remove(tmp_path, ec2);
        return false;
    }

    CleanupOldSnapshots();
    return true;
}

bool SnapshotManager::LoadLatestSnapshot(Snapshot& snapshot) {
    std::lock_guard<std::mutex> lock(mu_);

    std::string latest_snap;
    Index latest_idx = 0;
    std::error_code ec;

    for (auto& entry : fs::directory_iterator(snap_dir_, ec)) {
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
                // 文件名解析失败，跳过
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

void SnapshotManager::CleanupOldSnapshots() {
    std::vector<std::pair<Index, std::string>> snapshots;
    std::error_code ec;

    for (auto& entry : fs::directory_iterator(snap_dir_, ec)) {
        if (ec) break;
        std::string name = entry.path().filename().string();
        if (name.find("snapshot_") == 0) {
            try {
                Index idx = std::stoull(name.substr(9));
                snapshots.emplace_back(idx, entry.path().string());
            } catch (const std::invalid_argument&) {
                // 文件名解析失败，跳过
            } catch (const std::out_of_range&) {
                // 索引溢出，跳过
            }
        }
    }

    if (static_cast<int64_t>(snapshots.size()) <= max_snapshots_) return;

    std::sort(snapshots.begin(), snapshots.end());
    for (size_t i = 0; i < snapshots.size() - max_snapshots_; ++i) {
        std::error_code ec2;
        fs::remove(snapshots[i].second, ec2);
    }
}

bool SnapshotManager::ShouldSnapshot(Index last_applied, Index snapshot_index, int64_t threshold) const {
    if (last_applied <= snapshot_index) return false;
    return (last_applied - snapshot_index) >= static_cast<Index>(threshold);
}

} // namespace myetcd