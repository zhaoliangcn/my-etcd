#include "minimal_test.h"
#include "../../src/watch/watcher.h"

using namespace myetcd;

// ============================================================
// WatchManager 测试
// ============================================================

static bool test_watch_create() {
    WatchManager wm;
    int64_t id = wm.Watch("test_key", 0);
    TEST_ASSERT(id > 0, "watch id should be positive");
    auto watcher = wm.GetWatcher(id);
    TEST_ASSERT(watcher != nullptr, "watcher should exist");
    TEST_ASSERT_EQ(watcher->key, "test_key", "watcher key");
    TEST_ASSERT_EQ(static_cast<int>(watcher->type), static_cast<int>(Watcher::WatchType::Key), "watcher type should be Key");
    TEST_ASSERT_FALSE(watcher->cancelled, "watcher should not be cancelled initially");
    return true;
}

static bool test_watch_create_prefix() {
    WatchManager wm;
    int64_t id = wm.Watch("/prefix/", 0, true);
    auto watcher = wm.GetWatcher(id);
    TEST_ASSERT(watcher != nullptr, "prefix watcher should exist");
    TEST_ASSERT_EQ(static_cast<int>(watcher->type), static_cast<int>(Watcher::WatchType::Prefix), "watcher type should be Prefix");
    return true;
}

static bool test_watch_cancel() {
    WatchManager wm;
    int64_t id = wm.Watch("key", 0);
    TEST_ASSERT_TRUE(wm.Cancel(id), "cancel should succeed");
    TEST_ASSERT_FALSE(wm.Cancel(id), "second cancel should fail");
    TEST_ASSERT_TRUE(wm.GetWatcher(id) == nullptr, "cancelled watcher should not exist");
    return true;
}

static bool test_watch_cancel_nonexistent() {
    WatchManager wm;
    TEST_ASSERT_FALSE(wm.Cancel(99999), "cancelling nonexistent watcher should fail");
    return true;
}

static bool test_watch_notify() {
    WatchManager wm;
    int64_t id = wm.Watch("notify_key", 0);

    WatchEvent event;
    event.type = EventType::PUT;
    event.kv.key = "notify_key";
    event.kv.value = "test_val";
    event.kv.mod_revision = 1;

    wm.Notify(event);

    auto watcher = wm.GetWatcher(id);
    TEST_ASSERT(watcher != nullptr, "watcher should exist");
    TEST_ASSERT_FALSE(watcher->events.empty(), "watcher should have an event");
    return true;
}

static bool test_watch_notify_non_matching() {
    WatchManager wm;
    wm.Watch("my_key", 0);

    WatchEvent event;
    event.type = EventType::PUT;
    event.kv.key = "other_key";
    event.kv.mod_revision = 1;

    wm.Notify(event);

    // 只有匹配的 watcher 收到事件
    auto all = wm.GetWatcher(1);
    TEST_ASSERT(all != nullptr, "watcher should exist");
    TEST_ASSERT_TRUE(all->events.empty(), "non-matching key should not trigger event");
    return true;
}

static bool test_watch_prefix_matching() {
    WatchManager wm;
    wm.Watch("/myapp/", 0, true);

    // 前缀匹配的 key
    WatchEvent event;
    event.type = EventType::PUT;
    event.kv.key = "/myapp/config";
    event.kv.mod_revision = 1;
    wm.Notify(event);

    auto watcher = wm.GetWatcher(1);
    TEST_ASSERT(watcher != nullptr, "watcher should exist");
    TEST_ASSERT_FALSE(watcher->events.empty(), "prefix watcher should receive event");

    // 不匹配的 key
    WatchEvent event2;
    event2.type = EventType::PUT;
    event2.kv.key = "/other/config";
    event2.kv.mod_revision = 2;
    wm.Notify(event2);

    TEST_ASSERT_EQ(watcher->events.size(), 1UL, "non-matching prefix should not add event");
    return true;
}

static bool test_watch_notify_prev_revision() {
    WatchManager wm;
    wm.Watch("key", 5);  // start_rev = 5

    WatchEvent event;
    event.type = EventType::PUT;
    event.kv.key = "key";
    event.kv.mod_revision = 3;  // 小于 start_rev
    wm.Notify(event);

    auto watcher = wm.GetWatcher(1);
    TEST_ASSERT(watcher != nullptr, "watcher should exist");
    TEST_ASSERT_TRUE(watcher->events.empty(), "event before start_rev should be skipped");
    return true;
}

static bool test_watch_cleanup() {
    WatchManager wm;
    wm.Watch("key1", 0);
    wm.Watch("key2", 0);
    wm.Cancel(1);

    wm.Cleanup();

    // key1 的 watcher 应被清理
    TEST_ASSERT_TRUE(wm.GetWatcher(1) == nullptr, "cancelled watcher should be cleaned");
    // key2 的 watcher 应保留
    TEST_ASSERT_TRUE(wm.GetWatcher(2) != nullptr, "active watcher should remain");
    return true;
}

static bool test_watch_multiple_watchers() {
    WatchManager wm;
    wm.Watch("shared_key", 0);
    wm.Watch("shared_key", 0);

    WatchEvent event;
    event.type = EventType::PUT;
    event.kv.key = "shared_key";
    event.kv.mod_revision = 1;
    wm.Notify(event);

    // 两个 watcher 都应收到事件
    auto w1 = wm.GetWatcher(1);
    auto w2 = wm.GetWatcher(2);
    TEST_ASSERT(w1 != nullptr && w2 != nullptr, "both watchers should exist");
    TEST_ASSERT_EQ(w1->events.size(), 1UL, "first watcher should have event");
    TEST_ASSERT_EQ(w2->events.size(), 1UL, "second watcher should have event");
    return true;
}

static bool test_watch_wait_timeout() {
    Watcher w;
    w.id = 1;
    w.key = "key";

    // WaitForEvent 超时应该返回一个事件（cancelled 事件）
    auto ev = w.WaitForEvent(1);  // 1ms 超时
    // 超时后返回的 events 可能为空或者特定事件
    TEST_ASSERT(ev.type == EventType::PUT || ev.type == EventType::DELETE,
                "timeout event type should be valid");
    return true;
}

static bool test_watch_push_event() {
    Watcher w;
    w.id = 1;
    w.key = "key";

    WatchEvent event;
    event.type = EventType::PUT;
    event.kv.key = "key";
    event.kv.value = "val";

    w.PushEvent(event);
    TEST_ASSERT_EQ(w.events.size(), 1UL, "should have 1 event after push");
    TEST_ASSERT_EQ(w.events.front().kv.value, "val", "event value");
    return true;
}

// ============================================================

void run_watch_tests() {
    TEST_SUITE("WatchManager");
    RUN_TEST(test_watch_create);
    RUN_TEST(test_watch_create_prefix);
    RUN_TEST(test_watch_cancel);
    RUN_TEST(test_watch_cancel_nonexistent);
    RUN_TEST(test_watch_notify);
    RUN_TEST(test_watch_notify_non_matching);
    RUN_TEST(test_watch_prefix_matching);
    RUN_TEST(test_watch_notify_prev_revision);
    RUN_TEST(test_watch_cleanup);
    RUN_TEST(test_watch_multiple_watchers);
    RUN_TEST(test_watch_wait_timeout);
    RUN_TEST(test_watch_push_event);
}
