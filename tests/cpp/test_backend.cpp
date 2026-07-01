#include "minimal_test.h"
#include "../../src/storage/backend.h"
#include <filesystem>
#include <cstdio>

using namespace myetcd;
namespace fs = std::filesystem;

static std::string test_dir() {
    static int counter = 0;
    return "/tmp/myetcd_test_backend_" + std::to_string(++counter);
}

static void cleanup(const std::string& dir) {
    std::error_code ec;
    fs::remove_all(dir, ec);
}

// ============================================================
// Backend 测试
// ============================================================

static bool test_backend_open_close() {
    std::string dir = test_dir();
    {
        Backend backend(dir);
        TEST_ASSERT_TRUE(backend.Open(), "open should succeed");
        TEST_ASSERT_TRUE(fs::exists(dir), "data dir should be created");
    }
    cleanup(dir);
    return true;
}

static bool test_backend_put_and_get() {
    std::string dir = test_dir();
    {
        Backend backend(dir);
        backend.Open();
        TEST_ASSERT_TRUE(backend.Put("key1", "value1"), "put should succeed");
        TEST_ASSERT_TRUE(backend.Put("key2", "value2"), "put should succeed");
    }
    {
        // 重新打开，验证持久化
        Backend backend(dir);
        backend.Open();
        auto val = backend.Get("key1");
        TEST_ASSERT_TRUE(val.has_value(), "key1 should exist");
        TEST_ASSERT_EQ(*val, "value1", "key1 value");

        val = backend.Get("key2");
        TEST_ASSERT_TRUE(val.has_value(), "key2 should exist");
        TEST_ASSERT_EQ(*val, "value2", "key2 value");

        val = backend.Get("nonexistent");
        TEST_ASSERT_FALSE(val.has_value(), "nonexistent should not exist");
    }
    cleanup(dir);
    return true;
}

static bool test_backend_delete() {
    std::string dir = test_dir();
    Backend backend(dir);
    backend.Open();
    backend.Put("key", "val");
    TEST_ASSERT_TRUE(backend.Delete("key"), "delete should succeed");
    auto val = backend.Get("key");
    TEST_ASSERT_FALSE(val.has_value(), "deleted key should not exist");
    cleanup(dir);
    return true;
}

static bool test_backend_overwrite() {
    std::string dir = test_dir();
    Backend backend(dir);
    backend.Open();
    backend.Put("key", "v1");
    backend.Put("key", "v2");
    auto val = backend.Get("key");
    TEST_ASSERT_TRUE(val.has_value(), "key should exist");
    TEST_ASSERT_EQ(*val, "v2", "overwritten value should be v2");
    cleanup(dir);
    return true;
}

static bool test_backend_range() {
    std::string dir = test_dir();
    Backend backend(dir);
    backend.Open();
    backend.Put("a", "1");
    backend.Put("b", "2");
    backend.Put("c", "3");
    backend.Put("d", "4");
    backend.Put("e", "5");

    auto kvs = backend.Range("b", "d");
    TEST_ASSERT_EQ(kvs.size(), 2UL, "range [b, d) should have 2 keys");
    TEST_ASSERT_EQ(kvs[0].key, "b", "first key");
    TEST_ASSERT_EQ(kvs[1].key, "c", "second key");

    kvs = backend.Range("a", "f", 3);
    TEST_ASSERT_EQ(kvs.size(), 3UL, "limited range should have 3 keys");
    cleanup(dir);
    return true;
}

static bool test_backend_prefix_range() {
    std::string dir = test_dir();
    Backend backend(dir);
    backend.Open();
    backend.Put("/prefix/x", "1");
    backend.Put("/prefix/y", "2");
    backend.Put("/other/z", "3");

    auto kvs = backend.PrefixRange("/prefix/");
    TEST_ASSERT_EQ(kvs.size(), 2UL, "prefix /prefix/ should have 2 keys");

    kvs = backend.PrefixRange("/nonexistent");
    TEST_ASSERT_EQ(kvs.size(), 0UL, "nonexistent prefix should be empty");
    cleanup(dir);
    return true;
}

static bool test_backend_batch_put() {
    std::string dir = test_dir();
    Backend backend(dir);
    backend.Open();

    std::map<std::string, std::string> batch;
    batch["k1"] = "v1";
    batch["k2"] = "v2";
    batch["k3"] = "v3";
    TEST_ASSERT_TRUE(backend.BatchPut(batch), "batch put should succeed");

    for (const auto& [k, v] : batch) {
        auto val = backend.Get(k);
        TEST_ASSERT_TRUE(val.has_value(), ("key " + k + " should exist").c_str());
        TEST_ASSERT_EQ(*val, v, ("value for " + k).c_str());
    }
    cleanup(dir);
    return true;
}

static bool test_backend_sync() {
    std::string dir = test_dir();
    Backend backend(dir);
    backend.Open();
    backend.Put("key", "val");
    TEST_ASSERT_TRUE(backend.Sync(), "sync should succeed");
    cleanup(dir);
    return true;
}

static bool test_backend_persistence_on_close() {
    std::string dir = test_dir();
    {
        Backend backend(dir);
        backend.Open();
        backend.Put("persist", "data");
        // Close 时应该刷盘
    }
    {
        Backend backend(dir);
        backend.Open();
        auto val = backend.Get("persist");
        TEST_ASSERT_TRUE(val.has_value(), "persisted key should exist after reopen");
        TEST_ASSERT_EQ(*val, "data", "persisted value");
    }
    cleanup(dir);
    return true;
}

// ============================================================

void run_backend_tests() {
    TEST_SUITE("Backend");
    RUN_TEST(test_backend_open_close);
    RUN_TEST(test_backend_put_and_get);
    RUN_TEST(test_backend_delete);
    RUN_TEST(test_backend_overwrite);
    RUN_TEST(test_backend_range);
    RUN_TEST(test_backend_prefix_range);
    RUN_TEST(test_backend_batch_put);
    RUN_TEST(test_backend_sync);
    RUN_TEST(test_backend_persistence_on_close);
}
