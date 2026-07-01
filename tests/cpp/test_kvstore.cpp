#include "minimal_test.h"
#include "../../src/storage/kvstore.h"
#include <filesystem>

using namespace myetcd;
namespace fs = std::filesystem;

static std::string test_dir() {
    static int counter = 0;
    return "/tmp/myetcd_test_kvstore_" + std::to_string(++counter);
}

// ============================================================
// KVStore 测试（MVCC + Backend 集成）
// ============================================================

static bool test_kvstore_put_and_get() {
    std::string dir = test_dir();
    {
        KVStore kv(dir);
        kv.Open();

        kv.Put("key", "value");
        auto kvp = kv.Get("key");
        TEST_ASSERT_TRUE(kvp.has_value(), "key should exist");
        TEST_ASSERT_EQ(kvp->key, "key", "key name");
        TEST_ASSERT_EQ(kvp->value, "value", "key value");
    }
    // 清理
    std::error_code ec;
    fs::remove_all(dir, ec);
    return true;
}

static bool test_kvstore_put_overwrite() {
    std::string dir = test_dir();
    KVStore kv(dir);
    kv.Open();

    kv.Put("key", "v1");
    kv.Put("key", "v2");

    auto kvp = kv.Get("key");
    TEST_ASSERT_TRUE(kvp.has_value(), "key should exist");
    TEST_ASSERT_EQ(kvp->value, "v2", "latest value should be v2");
    TEST_ASSERT(kvp->mod_revision > kvp->create_revision, "mod_revision > create_revision");
    TEST_ASSERT(kvp->version >= 2, "version should be >= 2");

    std::error_code ec;
    fs::remove_all(dir, ec);
    return true;
}

static bool test_kvstore_delete() {
    std::string dir = test_dir();
    KVStore kv(dir);
    kv.Open();

    kv.Put("key", "val");
    TEST_ASSERT_TRUE(kv.Get("key").has_value(), "key should exist before delete");

    kv.Delete("key");
    TEST_ASSERT_FALSE(kv.Get("key").has_value(), "key should not exist after delete");

    std::error_code ec;
    fs::remove_all(dir, ec);
    return true;
}

static bool test_kvstore_get_nonexistent() {
    std::string dir = test_dir();
    KVStore kv(dir);
    kv.Open();
    auto kvp = kv.Get("nonexistent");
    TEST_ASSERT_FALSE(kvp.has_value(), "nonexistent key should not exist");

    std::error_code ec;
    fs::remove_all(dir, ec);
    return true;
}

static bool test_kvstore_range() {
    std::string dir = test_dir();
    KVStore kv(dir);
    kv.Open();
    kv.Put("a", "1");
    kv.Put("b", "2");
    kv.Put("c", "3");
    kv.Put("d", "4");

    auto kvs = kv.Range("b", "d");
    TEST_ASSERT_EQ(kvs.size(), 2UL, "range [b,d) should have 2 entries");

    std::error_code ec;
    fs::remove_all(dir, ec);
    return true;
}

static bool test_kvstore_prefix_range() {
    std::string dir = test_dir();
    KVStore kv(dir);
    kv.Open();
    kv.Put("/p/a", "1");
    kv.Put("/p/b", "2");
    kv.Put("/p/c", "3");
    kv.Put("/other", "4");

    auto kvs = kv.PrefixRange("/p/");
    TEST_ASSERT_EQ(kvs.size(), 3UL, "prefix /p/ should have 3 entries");

    kvs = kv.PrefixRange("/nonexistent");
    TEST_ASSERT_EQ(kvs.size(), 0UL, "nonexistent prefix should be empty");

    std::error_code ec;
    fs::remove_all(dir, ec);
    return true;
}

static bool test_kvstore_keys() {
    std::string dir = test_dir();
    KVStore kv(dir);
    kv.Open();
    kv.Put("k1", "v1");
    kv.Put("k2", "v2");
    kv.Put("k3", "v3");

    auto keys = kv.Keys();
    TEST_ASSERT_EQ(keys.size(), 3UL, "should have 3 keys");

    kv.Delete("k2");
    keys = kv.Keys();
    TEST_ASSERT_EQ(keys.size(), 2UL, "should have 2 keys after delete");

    std::error_code ec;
    fs::remove_all(dir, ec);
    return true;
}

static bool test_kvstore_restore_from_entries() {
    std::string dir = test_dir();
    KVStore kv(dir);
    kv.Open();

    std::vector<RaftEntry> entries;
    RaftEntry e1;
    e1.type = EventType::PUT;
    e1.index = 1;
    e1.key = "restored_key";
    e1.value = "restored_val";
    entries.push_back(e1);

    RaftEntry e2;
    e2.type = EventType::PUT;
    e2.index = 2;
    e2.key = "another_key";
    e2.value = "another_val";
    entries.push_back(e2);

    kv.RestoreFromEntries(entries);

    auto kvp = kv.Get("restored_key");
    TEST_ASSERT_TRUE(kvp.has_value(), "restored key should exist");
    TEST_ASSERT_EQ(kvp->value, "restored_val", "restored value");

    kvp = kv.Get("another_key");
    TEST_ASSERT_TRUE(kvp.has_value(), "second restored key should exist");

    std::error_code ec;
    fs::remove_all(dir, ec);
    return true;
}

static bool test_kvstore_serialize_deserialize() {
    std::string dir = test_dir();
    KVStore kv(dir);
    kv.Open();
    kv.Put("k1", "v1");
    kv.Put("k2", "v2");

    auto data = kv.Serialize();
    TEST_ASSERT_FALSE(data.empty(), "serialized data should not be empty");

    KVStore kv2(dir + "_2");
    kv2.Open();
    TEST_ASSERT_TRUE(kv2.Deserialize(data), "deserialize should succeed");

    auto kvp = kv2.Get("k1");
    TEST_ASSERT_TRUE(kvp.has_value(), "k1 should exist after deserialize");
    TEST_ASSERT_EQ(kvp->value, "v1", "k1 value after deserialize");

    kvp = kv2.Get("k2");
    TEST_ASSERT_TRUE(kvp.has_value(), "k2 should exist after deserialize");
    TEST_ASSERT_EQ(kvp->value, "v2", "k2 value after deserialize");

    std::error_code ec;
    fs::remove_all(dir, ec);
    fs::remove_all(dir + "_2", ec);
    return true;
}

static bool test_kvstore_revision_allocator() {
    std::string dir = test_dir();
    KVStore kv(dir);
    kv.Open();

    auto rev1 = kv.AllocateRevision();
    auto rev2 = kv.AllocateRevision();
    auto rev3 = kv.AllocateRevision();

    TEST_ASSERT(rev1 > 0, "first revision should be positive");
    TEST_ASSERT(rev2 > rev1, "revisions should be monotonic");
    TEST_ASSERT(rev3 > rev2, "revisions should be monotonic");

    std::error_code ec;
    fs::remove_all(dir, ec);
    return true;
}

static bool test_kvstore_compact() {
    std::string dir = test_dir();
    KVStore kv(dir);
    kv.Open();

    kv.Put("key", "v1");
    kv.Put("key", "v2");
    kv.Put("key", "v3");

    // 压缩到 CurrentRevision-1，保留最新版本 (rev 3, value=v3)
    Revision current = kv.CurrentRevision();  // = 4
    kv.Compact(current - 1);  // 清除 rev < 3 的版本

    // compact 后最新版本应仍然可读
    auto kvp = kv.Get("key");
    TEST_ASSERT_TRUE(kvp.has_value(), "key should exist after compact");
    TEST_ASSERT_EQ(kvp->value, "v3", "latest value after compact");

    std::error_code ec;
    fs::remove_all(dir, ec);
    return true;
}

static bool test_kvstore_current_revision() {
    std::string dir = test_dir();
    KVStore kv(dir);
    kv.Open();

    // 初始 revision 应为 1
    TEST_ASSERT(kv.CurrentRevision() >= 1, "current revision should be >= 1");

    kv.Put("k", "v");
    TEST_ASSERT(kv.CurrentRevision() > 1, "revision should increase after put");

    std::error_code ec;
    fs::remove_all(dir, ec);
    return true;
}

// ============================================================

void run_kvstore_tests() {
    TEST_SUITE("KVStore (MVCC + Backend)");
    RUN_TEST(test_kvstore_put_and_get);
    RUN_TEST(test_kvstore_put_overwrite);
    RUN_TEST(test_kvstore_delete);
    RUN_TEST(test_kvstore_get_nonexistent);
    RUN_TEST(test_kvstore_range);
    RUN_TEST(test_kvstore_prefix_range);
    RUN_TEST(test_kvstore_keys);
    RUN_TEST(test_kvstore_restore_from_entries);
    RUN_TEST(test_kvstore_serialize_deserialize);
    RUN_TEST(test_kvstore_revision_allocator);
    RUN_TEST(test_kvstore_compact);
    RUN_TEST(test_kvstore_current_revision);
}
