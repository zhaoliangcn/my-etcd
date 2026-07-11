#include "watch/watcher.h"
#include <chrono>
#include <optional>

namespace myetcd {

std::optional<WatchEvent> Watcher::WaitForEvent(int64_t timeout_ms) {
    std::unique_lock<std::mutex> lock(event_mu);

    if (timeout_ms < 0) {
        event_cv.wait(lock, [this] { return !events.empty() || cancelled; });
    } else {
        event_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
            [this] { return !events.empty() || cancelled; });
    }

    if (events.empty()) {
        return std::nullopt;
    }

    WatchEvent ev = events.front();
    events.pop();
    return ev;
}

void Watcher::PushEvent(const WatchEvent& event) {
    {
        std::lock_guard<std::mutex> lock(event_mu);
        // 队列满时丢弃最旧事件，防止 OOM
        while (events.size() >= kMaxEventQueueSize) {
            events.pop();
        }
        events.push(event);
    }
    event_cv.notify_one();
}

WatchManager::WatchManager() {}

int64_t WatchManager::Watch(const std::string& key, Revision start_rev, bool prefix) {
    std::lock_guard<std::mutex> lock(mu_);

    int64_t id = next_id_++;
    auto w = std::make_shared<Watcher>();
    w->id = id;
    w->key = key;
    w->type = prefix ? Watcher::WatchType::Prefix : Watcher::WatchType::Key;
    w->start_rev = start_rev;
    watchers_[id] = w;

    return id;
}

bool WatchManager::Cancel(int64_t watch_id) {
    std::shared_ptr<Watcher> w;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = watchers_.find(watch_id);
        if (it == watchers_.end()) return false;
        w = it->second;
        watchers_.erase(it);
    }

    // 在 mu_ 外设置 cancelled，但需要持有 event_mu 以避免与 PushEvent 竞争
    {
        std::lock_guard<std::mutex> elock(w->event_mu);
        w->cancelled = true;
    }
    w->event_cv.notify_all();
    return true;
}

void WatchManager::Notify(const WatchEvent& event) {
    std::lock_guard<std::mutex> lock(mu_);

    for (auto& [id, w] : watchers_) {
        if (w->cancelled) continue;
        if (event.kv.mod_revision <= w->start_rev) continue;

        bool match = false;
        if (w->type == Watcher::WatchType::Key) {
            match = (event.kv.key == w->key);
        } else {
            match = (event.kv.key.compare(0, w->key.size(), w->key) == 0);
        }

        if (match) {
            w->PushEvent(event);
        }
    }
}

std::shared_ptr<Watcher> WatchManager::GetWatcher(int64_t watch_id) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = watchers_.find(watch_id);
    if (it != watchers_.end()) {
        return it->second;
    }
    return nullptr;
}

void WatchManager::Cleanup() {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = watchers_.begin();
    while (it != watchers_.end()) {
        if (it->second->cancelled) {
            it = watchers_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace myetcd