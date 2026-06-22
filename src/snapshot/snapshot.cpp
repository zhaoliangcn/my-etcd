#include "snapshot/snapshot.h"
#include <filesystem>
#include <fstream>
#include <algorithm>

namespace myetcd {

namespace fs = std::filesystem;

SnapshotManager::SnapshotManager(const std::string& snap_dir, int64_t max_snapshots)
    : snap_dir_(snap_dir), max_snapshots_(max_snapshots)
{
    std::error_code ec;
    if (!fs::exists(snap_dir_, ec)) {
        fs::create_directories(snap_dir_, ec);
    }
}

bool SnapshotManager::CreateSnapshot(Index last_index, Term last_term, const std::vector<uint8_t>& data) {
    std::lock_guard<std::mutex> lock(mu_);

    std::string path = snap_dir_ + "/snapshot_" + std::to_string(last_index);
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    if (!ofs) return false;

    ofs.write(reinterpret_cast<const char*>(&last_index), sizeof(Index));
    ofs.write(reinterpret_cast<const char*>(&last_term), sizeof(Term));

    uint32_t data_size = static_cast<uint32_t>(data.size());
    ofs.write(reinterpret_cast<const char*>(&data_size), sizeof(uint32_t));
    if (!data.empty()) {
        ofs.write(reinterpret_cast<const char*>(data.data()), data.size());
    }

    ofs.close();
    if (!ofs.good()) return false;

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
            } catch (...) {}
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
            } catch (...) {}
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
    return (last_applied - snapshot_index) >= static_cast<Index>(threshold);
}

} // namespace myetcd