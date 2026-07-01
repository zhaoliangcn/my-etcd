#include "minimal_test.h"

// 各测试套件入口
void run_mvcc_tests();
void run_backend_tests();
void run_lease_tests();
void run_watch_tests();
void run_kvstore_tests();
void run_raft_log_tests();

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    std::cout << "============================================" << std::endl;
    std::cout << "  my-etcd C++ Unit Tests" << std::endl;
    std::cout << "============================================" << std::endl;

    int total_passed = 0, total_failed = 0;

    // 运行所有测试套件
    {
        g_passed = 0;
        g_failed = 0;
        run_mvcc_tests();
        total_passed += g_passed;
        total_failed += g_failed;
    }
    {
        g_passed = 0;
        g_failed = 0;
        run_backend_tests();
        total_passed += g_passed;
        total_failed += g_failed;
    }
    {
        g_passed = 0;
        g_failed = 0;
        run_lease_tests();
        total_passed += g_passed;
        total_failed += g_failed;
    }
    {
        g_passed = 0;
        g_failed = 0;
        run_watch_tests();
        total_passed += g_passed;
        total_failed += g_failed;
    }
    {
        g_passed = 0;
        g_failed = 0;
        run_kvstore_tests();
        total_passed += g_passed;
        total_failed += g_failed;
    }
    {
        g_passed = 0;
        g_failed = 0;
        run_raft_log_tests();
        total_passed += g_passed;
        total_failed += g_failed;
    }

    std::cout << "\n============================================" << std::endl;
    std::cout << "  Summary: " << (total_passed + total_failed)
              << " tests, " << total_passed << " passed, "
              << total_failed << " failed" << std::endl;
    std::cout << "============================================" << std::endl;

    return total_failed > 0 ? 1 : 0;
}
