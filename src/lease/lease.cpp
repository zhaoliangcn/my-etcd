#include "lease/lease.h"
#include <iostream>

namespace myetcd {

LeaseManager::LeaseManager() {}

LeaseManager::~LeaseManager() {
    Stop();
}

void LeaseManager::Start() {
    running_ = true;
    expire_thread_ = std::thread(&LeaseManager::CheckExpiry, this);
}

void LeaseManager::Stop() {
    running_ = false;
    if (expire_thread_.joinable()) {
        expire_thread_.join();
    }
}

LeaseId LeaseManager::Grant(int64_t ttl_ms) {
    std::lock_guard<std::mutex> lock(mu_);

    LeaseId id = next_id_++;
    Lease lease;
    lease.id = id;
    lease.ttl_ms = ttl_ms;
    lease.remaining_ms = ttl_ms;
    lease.expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(ttl_ms);
    leases_[id] = lease;

    return id;
}

bool LeaseManager::Revoke(LeaseId id) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = leases_.find(id);
    if (it == leases_.end()) return false;

    // 通知所有关联的 key 过期
    if (expire_callback_) {
        for (const auto& key : it->second.attached_keys) {
            expire_callback_(key);
        }
    }

    leases_.erase(it);
    return true;
}

bool LeaseManager::Renew(LeaseId id) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = leases_.find(id);
    if (it == leases_.end()) return false;

    it->second.expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(it->second.ttl_ms);
    it->second.remaining_ms = it->second.ttl_ms;
    return true;
}

bool LeaseManager::Attach(LeaseId id, const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = leases_.find(id);
    if (it == leases_.end()) return false;

    it->second.attached_keys.insert(key);
    return true;
}

bool LeaseManager::Detach(LeaseId id, const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = leases_.find(id);
    if (it == leases_.end()) return false;

    it->second.attached_keys.erase(key);
    return true;
}

std::optional<Lease> LeaseManager::Get(LeaseId id) const {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = leases_.find(id);
    if (it == leases_.end()) return std::nullopt;

    auto lease = it->second;
    auto now = std::chrono::steady_clock::now();
    lease.remaining_ms = std::max<int64_t>(0,
        std::chrono::duration_cast<std::chrono::milliseconds>(lease.expiry - now).count());
    return lease;
}

std::set<std::string> LeaseManager::GetAttachedKeys(LeaseId id) const {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = leases_.find(id);
    if (it == leases_.end()) return {};

    return it->second.attached_keys;
}

void LeaseManager::SetExpireCallback(ExpireCallback callback) {
    std::lock_guard<std::mutex> lock(mu_);
    expire_callback_ = std::move(callback);
}

std::vector<Lease> LeaseManager::GetAllLeases() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<Lease> result;
    result.reserve(leases_.size());
    for (const auto& [id, lease] : leases_) {
        result.push_back(lease);
    }
    return result;
}

void LeaseManager::CheckExpiry() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(check_interval_ms_));

        // 在一次加锁中完成检查、回调和删除，消除 TOCTOU 竞态
        std::lock_guard<std::mutex> lock(mu_);
        auto now = std::chrono::steady_clock::now();
        auto it = leases_.begin();
        while (it != leases_.end()) {
            if (now >= it->second.expiry) {
                // 先触发过期回调
                if (expire_callback_) {
                    for (const auto& key : it->second.attached_keys) {
                        expire_callback_(key);
                    }
                }
                // 再删除租约
                it = leases_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

} // namespace myetcd