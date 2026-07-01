#pragma once

#include <iostream>
#include <string>
#include <functional>
#include <vector>
#include <sstream>

// 极简测试框架，零外部依赖

#define TEST_ASSERT(cond, msg) \
    do { \
        if (!(cond)) { \
            std::cerr << "  [FAIL] " << msg << " (" << __FILE__ << ":" << __LINE__ << ")" << std::endl; \
            return false; \
        } \
    } while(0)

#define TEST_ASSERT_EQ(a, b, msg) \
    TEST_ASSERT((a) == (b), msg << " - expected: " << (b) << ", got: " << (a))

#define TEST_ASSERT_NE(a, b, msg) \
    TEST_ASSERT((a) != (b), msg << " - values should differ")

#define TEST_ASSERT_TRUE(cond, msg) \
    TEST_ASSERT((cond), msg)

#define TEST_ASSERT_FALSE(cond, msg) \
    TEST_ASSERT(!(cond), msg)

#define RUN_TEST(name) \
    do { \
        std::cout << "  " << #name << "... " << std::flush; \
        if (name()) { \
            std::cout << "PASS" << std::endl; \
            g_passed++; \
        } else { \
            std::cout << "FAIL" << std::endl; \
            g_failed++; \
        } \
    } while(0)

#define TEST_SUITE(name) \
    std::cout << "\n=== " << name << " ===" << std::endl;

inline int g_passed = 0, g_failed = 0;
