#pragma once

#include "common/types.h"
#include <map>
#include <set>
#include <mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include <functional>

namespace myetcd {

// 租约信息
struct Lease {
    LeaseId id;
    int64_t ttl_ms;           // 原始 TTL (毫秒)
    int64_t remaining_ms;     // 剩余时间 (毫秒)
    std::chrono::steady_clock::time_point expiry;
    std::set<std::string> attached_keys;
};

// 租约管理器
class LeaseManager {
public:
    using ExpireCallback = std::function<void(const std::string& key)>;

    LeaseManager();
    ~LeaseManager();

    LeaseManager(const LeaseManager&) = delete;
    LeaseManager& operator=(const LeaseManager&) = delete;

    // 启动过期检查线程
    void Start();

    // 停止
    void Stop();

    // 创建租约
    LeaseId Grant(int64_t ttl_ms);

    // 撤销租约
    bool Revoke(LeaseId id);

    // 续约
    bool Renew(LeaseId id);

    // 将 key 关联到租约
    bool Attach(LeaseId id, const std::string& key);

    // 解除 key 与租约的关联
    bool Detach(LeaseId id, const std::string& key);

    // 获取租约信息
    std::optional<Lease> Get(LeaseId id) const;

    // 获取租约关联的 key
    std::set<std::string> GetAttachedKeys(LeaseId id) const;

    // 设置过期回调
    void SetExpireCallback(ExpireCallback callback);

    // 获取所有未过期的租约
    std::vector<Lease> GetAllLeases() const;

private:
    void CheckExpiry();

    mutable std::mutex mu_;
    std::map<LeaseId, Lease> leases_;
    LeaseId next_id_ = 1;
    ExpireCallback expire_callback_;

    std::thread expire_thread_;
    std::atomic<bool> running_{false};
    int64_t check_interval_ms_ = 500; // 每 500ms 检查一次
};

} // namespace myetcd