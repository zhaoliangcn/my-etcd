#pragma once

#include "common/types.h"
#include <map>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <atomic>

namespace myetcd {

// Watcher - 监听单个 key 或前缀
struct Watcher {
    enum class WatchType {
        Key,
        Prefix,
    };

    static constexpr size_t kMaxEventQueueSize = 1024;

    int64_t id = 0;
    std::string key;
    WatchType type = WatchType::Key;
    Revision start_rev = 0;
    std::queue<WatchEvent> events;
    std::mutex event_mu;
    std::condition_variable event_cv;
    bool cancelled = false;

    // 等待事件，返回 nullopt 表示超时或取消
    std::optional<WatchEvent> WaitForEvent(int64_t timeout_ms = -1);

    // 推送事件，队列满时丢弃最旧的事件
    void PushEvent(const WatchEvent& event);
};

// Watch 管理器
class WatchManager {
public:
    WatchManager();
    ~WatchManager() = default;

    // 创建 Watcher
    int64_t Watch(const std::string& key, Revision start_rev, bool prefix = false);

    // 取消 Watcher
    bool Cancel(int64_t watch_id);

    // 通知所有匹配的 Watcher
    void Notify(const WatchEvent& event);

    // 获取 Watcher
    std::shared_ptr<Watcher> GetWatcher(int64_t watch_id);

    // 清理已取消的 Watcher
    void Cleanup();

private:
    mutable std::mutex mu_;
    std::map<int64_t, std::shared_ptr<Watcher>> watchers_;
    std::atomic<int64_t> next_id_{1};
};

} // namespace myetcd