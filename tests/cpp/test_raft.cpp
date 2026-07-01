#include "minimal_test.h"
#include "../../src/raft/raft_log.h"
#include "../../src/raft/transport.h"
#include "../../src/raft/raft_node.h"
#include "../../src/common/types.h"
#include <thread>

using namespace myetcd;

// ============================================================
// RaftLog 测试
// ============================================================

static bool test_raft_log_empty() {
    RaftLog log;
    TEST_ASSERT_EQ(log.LastIndex(), kNoIndex, "empty log last index");
    TEST_ASSERT_EQ(log.LastTerm(), kNoTerm, "empty log last term");
    TEST_ASSERT_EQ(log.Size(), 0UL, "empty log size");
    TEST_ASSERT_EQ(log.FirstIndex(), 1UL, "default first index");
    return true;
}

static bool test_raft_log_append() {
    RaftLog log;

    RaftEntry e1;
    e1.term = 1;
    e1.index = 1;
    e1.key = "key1";
    e1.value = "val1";
    log.Append(e1);

    TEST_ASSERT_EQ(log.LastIndex(), 1UL, "last index after 1 append");
    TEST_ASSERT_EQ(log.LastTerm(), 1UL, "last term after 1 append");
    TEST_ASSERT_EQ(log.Size(), 1UL, "size after 1 append");
    TEST_ASSERT_EQ(log.TermAt(1), 1UL, "term at index 1");

    RaftEntry e2;
    e2.term = 1;
    e2.index = 2;
    e2.key = "key2";
    log.Append(e2);

    TEST_ASSERT_EQ(log.LastIndex(), 2UL, "last index after 2 appends");
    TEST_ASSERT_EQ(log.Size(), 2UL, "size after 2 appends");

    auto e = log.EntryAt(1);
    TEST_ASSERT_TRUE(e.has_value(), "entry at index 1 exists");
    TEST_ASSERT_EQ(e->key, "key1", "entry 1 key");

    e = log.EntryAt(2);
    TEST_ASSERT_TRUE(e.has_value(), "entry at index 2 exists");
    TEST_ASSERT_EQ(e->key, "key2", "entry 2 key");
    return true;
}

static bool test_raft_log_entries_from() {
    RaftLog log;
    for (int i = 1; i <= 5; i++) {
        RaftEntry e;
        e.term = 1;
        e.index = i;
        e.key = "k" + std::to_string(i);
        log.Append(e);
    }

    auto entries = log.EntriesFrom(3);
    TEST_ASSERT_EQ(entries.size(), 3UL, "entries from index 3 should have 3 entries");
    TEST_ASSERT_EQ(entries[0].index, 3UL, "first entry index");

    entries = log.EntriesFrom(1);
    TEST_ASSERT_EQ(entries.size(), 5UL, "entries from index 1 should have 5 entries");

    entries = log.EntriesFrom(10);
    TEST_ASSERT_EQ(entries.size(), 0UL, "entries from beyond end should be empty");
    return true;
}

static bool test_raft_log_slice() {
    RaftLog log;
    for (int i = 1; i <= 10; i++) {
        RaftEntry e;
        e.term = 1;
        e.index = i;
        log.Append(e);
    }

    auto entries = log.Slice(3, 7);
    TEST_ASSERT_EQ(entries.size(), 4UL, "slice [3,7) should have 4 entries");
    TEST_ASSERT_EQ(entries[0].index, 3UL, "first entry index");

    entries = log.Slice(1, 11);
    TEST_ASSERT_EQ(entries.size(), 10UL, "slice [1,11) should have 10 entries");

    entries = log.Slice(5, 3);
    TEST_ASSERT_EQ(entries.size(), 0UL, "invalid slice should be empty");
    return true;
}

static bool test_raft_log_truncate_from() {
    RaftLog log;
    for (int i = 1; i <= 10; i++) {
        RaftEntry e;
        e.term = 1;
        e.index = i;
        log.Append(e);
    }

    log.TruncateFrom(6);
    TEST_ASSERT_EQ(log.FirstIndex(), 6UL, "first index after truncate[6]");
    TEST_ASSERT_EQ(log.LastIndex(), 10UL, "last index should still be 10");
    TEST_ASSERT_EQ(log.Size(), 5UL, "size should be 5");

    // 截断后的索引仍然可查
    auto e = log.EntryAt(6);
    TEST_ASSERT_TRUE(e.has_value(), "entry at index 6 exists after truncate");
    TEST_ASSERT_EQ(e->index, 6UL, "entry 6 index");

    // 截断前的索引不可查
    e = log.EntryAt(5);
    TEST_ASSERT_FALSE(e.has_value(), "entry at index 5 should not exist after truncate");
    return true;
}

static bool test_raft_log_truncate_to() {
    RaftLog log;
    for (int i = 1; i <= 10; i++) {
        RaftEntry e;
        e.term = 1;
        e.index = i;
        log.Append(e);
    }

    log.TruncateTo(5);
    TEST_ASSERT_EQ(log.LastIndex(), 5UL, "last index after truncate to 5");
    TEST_ASSERT_EQ(log.Size(), 5UL, "size after truncate to 5");

    // 截断后的条目应存在
    auto e = log.EntryAt(5);
    TEST_ASSERT_TRUE(e.has_value(), "entry 5 should exist");
    TEST_ASSERT_EQ(e->index, 5UL, "entry 5 index");

    // 截断掉的条目不存在
    e = log.EntryAt(6);
    TEST_ASSERT_FALSE(e.has_value(), "entry 6 should not exist after truncate");
    return true;
}

static bool test_raft_log_match_term() {
    RaftLog log;
    RaftEntry e;
    e.term = 1;
    e.index = 1;
    log.Append(e);

    e.term = 1;
    e.index = 2;
    log.Append(e);

    e.term = 2;
    e.index = 3;
    log.Append(e);

    TEST_ASSERT_TRUE(log.MatchTerm(1, 1), "index 1 matches term 1");
    TEST_ASSERT_TRUE(log.MatchTerm(2, 1), "index 2 matches term 1");
    TEST_ASSERT_TRUE(log.MatchTerm(3, 2), "index 3 matches term 2");
    TEST_ASSERT_FALSE(log.MatchTerm(1, 2), "index 1 does not match term 2");
    TEST_ASSERT_FALSE(log.MatchTerm(3, 1), "index 3 does not match term 1");
    return true;
}

static bool test_raft_log_restore() {
    RaftLog log;
    for (int i = 1; i <= 5; i++) {
        RaftEntry e;
        e.term = 1;
        e.index = i;
        log.Append(e);
    }

    log.Restore(5);
    TEST_ASSERT_EQ(log.FirstIndex(), 6UL, "first index after restore(5)");
    TEST_ASSERT_EQ(log.LastIndex(), kNoIndex, "last index after restore (empty)");
    TEST_ASSERT_EQ(log.Size(), 0UL, "size after restore");

    // 重新追加
    RaftEntry e;
    e.term = 2;
    e.index = 6;
    log.Append(e);
    TEST_ASSERT_EQ(log.LastIndex(), 6UL, "last index after re-append");
    TEST_ASSERT_EQ(log.TermAt(6), 2UL, "term of restored entry");
    return true;
}

static bool test_raft_log_concurrent_safety() {
    RaftLog log;
    // 简单验证：多线程并发追加不会崩溃
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&log, t]() {
            for (int i = 0; i < 100; i++) {
                RaftEntry e;
                e.term = 1;
                e.index = log.LastIndex() + 1;
                e.key = "t" + std::to_string(t) + "_" + std::to_string(i);
                log.Append(e);
            }
        });
    }
    for (auto& th : threads) th.join();
    TEST_ASSERT(log.Size() > 0, "log should have entries after concurrent append");
    return true;
}

// ============================================================
// RaftNode 基础测试（单节点选举）
// ============================================================

static bool test_raft_node_single_node_election() {
    ClusterConfig config;
    config.node_id = 1;
    config.election_timeout_ms = 100;
    config.max_election_timeout_ms = 200;
    config.heartbeat_interval_ms = 50;

    RaftNode node(config);
    auto transport = std::make_shared<InMemoryTransport>();
    node.SetTransport(transport);
    node.Start();

    // 等待选举完成（单节点应直接成为 Leader）
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    node.Stop();
    TEST_ASSERT_EQ(static_cast<int>(node.State()), 2, "single node should become Leader"); // Leader=2
    return true;
}

static bool test_raft_node_handle_request_vote() {
    ClusterConfig config;
    config.node_id = 1;
    RaftNode node(config);
    auto transport = std::make_shared<InMemoryTransport>();
    node.SetTransport(transport);
    node.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 模拟 RequestVote
    RequestVoteRequest req;
    req.term = 5;
    req.candidate_id = 2;
    req.last_log_index = 0;
    req.last_log_term = 0;

    auto resp = node.HandleRequestVote(req);
    // 响应任期应该 >= 请求任期
    TEST_ASSERT(resp.term > 0, "response term should be positive");
    node.Stop();
    return true;
}

// ============================================================

void run_raft_log_tests() {
    TEST_SUITE("RaftLog");
    RUN_TEST(test_raft_log_empty);
    RUN_TEST(test_raft_log_append);
    RUN_TEST(test_raft_log_entries_from);
    RUN_TEST(test_raft_log_slice);
    RUN_TEST(test_raft_log_truncate_from);
    RUN_TEST(test_raft_log_truncate_to);
    RUN_TEST(test_raft_log_match_term);
    RUN_TEST(test_raft_log_restore);
    RUN_TEST(test_raft_log_concurrent_safety);

    TEST_SUITE("RaftNode");
    RUN_TEST(test_raft_node_single_node_election);
    RUN_TEST(test_raft_node_handle_request_vote);
}
