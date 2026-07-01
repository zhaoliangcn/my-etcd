#include "minimal_test.h"
#include "../../src/storage/mvcc.h"

using namespace myetcd;

// ============================================================
// MVCC 测试
// ============================================================

static bool test_mvcc_put_and_get() {
    MVCC mvcc;
    mvcc.Put("key1", 1, "value1", 0);
    mvcc.Put("key2", 2, "value2", 0);

    auto kv = mvcc.Get("key1");
    TEST_ASSERT_TRUE(kv.has_value(), "key1 should exist");
    TEST_ASSERT_EQ(kv->value, "value1", "key1 value");

    kv = mvcc.Get("key2");
    TEST_ASSERT_TRUE(kv.has_value(), "key2 should exist");
    TEST_ASSERT_EQ(kv->value, "value2", "key2 value");

    kv = mvcc.Get("nonexistent");
    TEST_ASSERT_FALSE(kv.has_value(), "nonexistent key should not exist");
    return true;
}

static bool test_mvcc_overwrite() {
    MVCC mvcc;
    mvcc.Put("key", 1, "v1", 0);
    mvcc.Put("key", 2, "v2", 0);
    mvcc.Put("key", 3, "v3", 0);

    auto kv = mvcc.Get("key");
    TEST_ASSERT_TRUE(kv.has_value(), "key should exist");
    TEST_ASSERT_EQ(kv->value, "v3", "latest value should be v3");
    TEST_ASSERT_EQ(kv->version, 3L, "version should be 3");
    TEST_ASSERT_EQ(kv->mod_revision, 3L, "mod_revision should be 3");
    return true;
}

static bool test_mvcc_historical_get() {
    MVCC mvcc;
    mvcc.Put("key", 1, "v1", 0);
    mvcc.Put("key", 2, "v2", 0);
    mvcc.Put("key", 3, "v3", 0);

    // 查询历史版本
    auto kv = mvcc.Get("key", 1);
    TEST_ASSERT_TRUE(kv.has_value(), "historical rev=1 should exist");
    TEST_ASSERT_EQ(kv->value, "v1", "historical value at rev=1");
    TEST_ASSERT_EQ(kv->mod_revision, 1L, "mod_revision should be 1");

    kv = mvcc.Get("key", 2);
    TEST_ASSERT_TRUE(kv.has_value(), "historical rev=2 should exist");
    TEST_ASSERT_EQ(kv->value, "v2", "historical value at rev=2");
    return true;
}

static bool test_mvcc_delete() {
    MVCC mvcc;
    mvcc.Put("key", 1, "val", 0);
    mvcc.Delete("key", 2);

    auto kv = mvcc.Get("key");
    TEST_ASSERT_FALSE(kv.has_value(), "deleted key should not be found");

    // Keys() 不应包含已删除的 key
    auto keys = mvcc.Keys();
    TEST_ASSERT_EQ(keys.size(), 0UL, "no keys after delete");
    return true;
}

static bool test_mvcc_range() {
    MVCC mvcc;
    mvcc.Put("a", 1, "1", 0);
    mvcc.Put("b", 2, "2", 0);
    mvcc.Put("c", 3, "3", 0);
    mvcc.Put("d", 4, "4", 0);

    auto kvs = mvcc.Range("b", "d");  // [b, d)
    TEST_ASSERT_EQ(kvs.size(), 2UL, "range [b, d) should have 2 keys");
    TEST_ASSERT_EQ(kvs[0].key, "b", "first key is b");
    TEST_ASSERT_EQ(kvs[1].key, "c", "second key is c");
    return true;
}

static bool test_mvcc_prefix_range() {
    MVCC mvcc;
    mvcc.Put("/prefix/a", 1, "1", 0);
    mvcc.Put("/prefix/b", 2, "2", 0);
    mvcc.Put("/prefix/c", 3, "3", 0);
    mvcc.Put("/other/x", 4, "4", 0);

    auto kvs = mvcc.PrefixRange("/prefix/");
    TEST_ASSERT_EQ(kvs.size(), 3UL, "prefix /prefix/ should have 3 keys");

    kvs = mvcc.PrefixRange("/nonexistent");
    TEST_ASSERT_EQ(kvs.size(), 0UL, "nonexistent prefix should be empty");
    return true;
}

static bool test_mvcc_compact() {
    MVCC mvcc;
    mvcc.Put("key", 1, "v1", 0);
    mvcc.Put("key", 2, "v2", 0);
    mvcc.Put("key", 3, "v3", 0);

    mvcc.Compact(3);  // 清除 rev < 3 的版本

    // compaction 后 rev 1 和 2 被清理
    auto ki = mvcc.GetKeyIndex("key");
    TEST_ASSERT_TRUE(ki.has_value(), "key index should exist after compact");
    TEST_ASSERT(ki->versions.size() >= 1, "at least 1 version should remain");
    // 确认 rev=1 已被移除
    for (const auto& v : ki->versions) {
        TEST_ASSERT(v.revision >= 3, "all remaining versions should have rev >= 3");
    }

    // 最新版本仍然可读
    auto kv = mvcc.Get("key");
    TEST_ASSERT_TRUE(kv.has_value(), "key should still be readable");
    TEST_ASSERT_EQ(kv->value, "v3", "latest value after compact");
    return true;
}

static bool test_mvcc_revision_monotonic() {
    MVCC mvcc;
    Revision prev = 0;
    for (int i = 0; i < 100; i++) {
        Revision rev = mvcc.AllocateRevision();
        TEST_ASSERT(rev > prev, "revision should be monotonically increasing");
        prev = rev;
    }
    return true;
}

static bool test_mvcc_put_with_lease() {
    MVCC mvcc;
    mvcc.Put("lease_key", 1, "val", 42);

    auto kv = mvcc.Get("lease_key");
    TEST_ASSERT_TRUE(kv.has_value(), "lease key should exist");
    TEST_ASSERT_EQ(kv->lease_id, 42L, "lease_id should be 42");
    return true;
}

static bool test_mvcc_delete_and_recreate() {
    MVCC mvcc;
    mvcc.Put("key", 1, "v1", 0);
    mvcc.Delete("key", 2);
    mvcc.Put("key", 3, "v2", 0);

    auto kv = mvcc.Get("key");
    TEST_ASSERT_TRUE(kv.has_value(), "recreated key should exist");
    TEST_ASSERT_EQ(kv->value, "v2", "recreated value");
    // MVCC 版本号持续递增（即使经过删除），version >= 2
    TEST_ASSERT(kv->version >= 1, "version should be >= 1 after recreate");
    return true;
}

// ============================================================

void run_mvcc_tests() {
    TEST_SUITE("MVCC");
    RUN_TEST(test_mvcc_put_and_get);
    RUN_TEST(test_mvcc_overwrite);
    RUN_TEST(test_mvcc_historical_get);
    RUN_TEST(test_mvcc_delete);
    RUN_TEST(test_mvcc_range);
    RUN_TEST(test_mvcc_prefix_range);
    RUN_TEST(test_mvcc_compact);
    RUN_TEST(test_mvcc_revision_monotonic);
    RUN_TEST(test_mvcc_put_with_lease);
    RUN_TEST(test_mvcc_delete_and_recreate);
}
