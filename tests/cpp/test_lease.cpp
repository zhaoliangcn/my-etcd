#include "minimal_test.h"
#include "../../src/lease/lease.h"

using namespace myetcd;

// ============================================================
// LeaseManager 测试
// ============================================================

static bool test_lease_grant() {
    LeaseManager lm;
    LeaseId id = lm.Grant(10000);
    TEST_ASSERT(id > 0, "lease id should be positive");
    auto lease = lm.Get(id);
    TEST_ASSERT_TRUE(lease.has_value(), "lease should exist");
    TEST_ASSERT_EQ(lease->ttl_ms, 10000L, "lease ttl");
    TEST_ASSERT_EQ(lease->id, id, "lease id matches");
    return true;
}

static bool test_lease_grant_multiple() {
    LeaseManager lm;
    LeaseId id1 = lm.Grant(1000);
    LeaseId id2 = lm.Grant(2000);
    LeaseId id3 = lm.Grant(3000);
    TEST_ASSERT(id3 > id2 && id2 > id1, "lease ids should be monotonically increasing");
    return true;
}

static bool test_lease_revoke() {
    LeaseManager lm;
    LeaseId id = lm.Grant(10000);
    TEST_ASSERT_TRUE(lm.Revoke(id), "revoke should succeed");
    TEST_ASSERT_FALSE(lm.Get(id).has_value(), "revoked lease should not exist");
    TEST_ASSERT_FALSE(lm.Revoke(id), "second revoke should fail");
    return true;
}

static bool test_lease_revoke_nonexistent() {
    LeaseManager lm;
    TEST_ASSERT_FALSE(lm.Revoke(99999), "revoking nonexistent lease should fail");
    return true;
}

static bool test_lease_renew() {
    LeaseManager lm;
    LeaseId id = lm.Grant(1000);
    auto lease = lm.Get(id);
    TEST_ASSERT_TRUE(lease.has_value(), "lease should exist");

    lm.Renew(id);
    auto renewed = lm.Get(id);
    TEST_ASSERT_TRUE(renewed.has_value(), "renewed lease should exist");
    TEST_ASSERT_EQ(renewed->id, id, "renewed lease id");
    TEST_ASSERT(renewed->remaining_ms > 0, "remaining should be positive after renew");
    return true;
}

static bool test_lease_renew_nonexistent() {
    LeaseManager lm;
    TEST_ASSERT_FALSE(lm.Renew(99999), "renewing nonexistent lease should return false");
    return true;
}

static bool test_lease_attach_detach() {
    LeaseManager lm;
    LeaseId id = lm.Grant(10000);

    TEST_ASSERT_TRUE(lm.Attach(id, "key1"), "attach key1");
    TEST_ASSERT_TRUE(lm.Attach(id, "key2"), "attach key2");

    auto keys = lm.GetAttachedKeys(id);
    TEST_ASSERT_EQ(keys.size(), 2UL, "should have 2 attached keys");
    TEST_ASSERT_TRUE(keys.count("key1") > 0, "key1 should be attached");
    TEST_ASSERT_TRUE(keys.count("key2") > 0, "key2 should be attached");

    TEST_ASSERT_TRUE(lm.Detach(id, "key1"), "detach key1");
    keys = lm.GetAttachedKeys(id);
    TEST_ASSERT_EQ(keys.size(), 1UL, "should have 1 key after detach");

    TEST_ASSERT_FALSE(lm.Attach(99999, "key"), "attach to nonexistent should fail");
    TEST_ASSERT_FALSE(lm.Detach(99999, "key"), "detach from nonexistent should fail");
    return true;
}

static bool test_lease_get_nonexistent() {
    LeaseManager lm;
    auto lease = lm.Get(99999);
    TEST_ASSERT_FALSE(lease.has_value(), "nonexistent lease should not exist");
    return true;
}

static bool test_lease_all_leases() {
    LeaseManager lm;
    lm.Grant(1000);
    lm.Grant(2000);
    lm.Grant(3000);

    auto all = lm.GetAllLeases();
    TEST_ASSERT_EQ(all.size(), 3UL, "should have 3 leases");
    return true;
}

static bool test_lease_expire_callback() {
    LeaseManager lm;
    std::vector<std::string> expired_keys;

    lm.SetExpireCallback([&](const std::string& key) {
        expired_keys.push_back(key);
    });

    LeaseId id = lm.Grant(100);  // 100ms TTL
    lm.Attach(id, "expire_key");

    // 手动触发过期（等待时间超过 TTL 后再调用 Revoke）
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 强制检查和撤销
    lm.Revoke(id);

    TEST_ASSERT_EQ(expired_keys.size(), 1UL, "expire callback should be called");
    if (!expired_keys.empty()) {
        TEST_ASSERT_EQ(expired_keys[0], "expire_key", "expired key name");
    }
    return true;
}

static bool test_lease_concurrent_access() {
    LeaseManager lm;
    std::vector<std::thread> threads;

    // 并发 Grant
    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&lm]() {
            for (int i = 0; i < 50; i++) {
                LeaseId id = lm.Grant(10000);
                lm.Attach(id, "key_" + std::to_string(id));
            }
        });
    }
    for (auto& th : threads) th.join();

    auto all = lm.GetAllLeases();
    TEST_ASSERT_EQ(all.size(), 200UL, "should have 200 leases");
    return true;
}

// ============================================================

void run_lease_tests() {
    TEST_SUITE("LeaseManager");
    RUN_TEST(test_lease_grant);
    RUN_TEST(test_lease_grant_multiple);
    RUN_TEST(test_lease_revoke);
    RUN_TEST(test_lease_revoke_nonexistent);
    RUN_TEST(test_lease_renew);
    RUN_TEST(test_lease_renew_nonexistent);
    RUN_TEST(test_lease_attach_detach);
    RUN_TEST(test_lease_get_nonexistent);
    RUN_TEST(test_lease_all_leases);
    RUN_TEST(test_lease_expire_callback);
    RUN_TEST(test_lease_concurrent_access);
}
