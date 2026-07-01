cmake_minimum_required(VERSION 3.16)
project(my-etcd VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 查找依赖
find_package(Threads REQUIRED)

# Protobuf & gRPC (可选，如果没有安装则使用纯 HTTP 模式)
find_package(Protobuf QUIET)
find_package(gRPC QUIET)

# 源文件
set(SOURCES
    src/main.cpp
    src/common/types.h
    src/raft/raft.h
    src/raft/raft.cpp
    src/raft/raft_log.h
    src/raft/raft_log.cpp
    src/raft/raft_node.h
    src/raft/transport.h
    src/raft/tcp_transport.h
    src/raft/tcp_transport.cpp
    src/wal/wal.h
    src/wal/wal.cpp
    src/snapshot/snapshot.h
    src/snapshot/snapshot.cpp
    src/storage/kvstore.h
    src/storage/kvstore.cpp
    src/storage/mvcc.h
    src/storage/mvcc.cpp
    src/storage/backend.h
    src/storage/backend.cpp
    src/lease/lease.h
    src/lease/lease.cpp
    src/watch/watcher.h
    src/watch/watcher.cpp
    src/server/etcd_server.h
    src/server/etcd_server.cpp
    src/server/http_handler.h
    src/server/http_handler.cpp
)

# 核心库源文件（不含 main.cpp，用于测试和主程序共享）
set(LIB_SOURCES
    src/common/types.h
    src/raft/raft.h
    src/raft/raft.cpp
    src/raft/raft_log.h
    src/raft/raft_log.cpp
    src/raft/raft_node.h
    src/raft/transport.h
    src/raft/tcp_transport.h
    src/raft/tcp_transport.cpp
    src/wal/wal.h
    src/wal/wal.cpp
    src/snapshot/snapshot.h
    src/snapshot/snapshot.cpp
    src/storage/kvstore.h
    src/storage/kvstore.cpp
    src/storage/mvcc.h
    src/storage/mvcc.cpp
    src/storage/backend.h
    src/storage/backend.cpp
    src/lease/lease.h
    src/lease/lease.cpp
    src/watch/watcher.h
    src/watch/watcher.cpp
    src/server/etcd_server.h
    src/server/etcd_server.cpp
    src/server/http_handler.h
    src/server/http_handler.cpp
)

# 核心静态库
add_library(my-etcd-core STATIC ${LIB_SOURCES})
target_include_directories(my-etcd-core PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(my-etcd-core PUBLIC Threads::Threads)

# 如果找到 gRPC
if(Protobuf_FOUND AND gRPC_FOUND)
    target_compile_definitions(my-etcd-core PRIVATE MYETCD_GRPC_SUPPORT)
    target_link_libraries(my-etcd-core PUBLIC protobuf::libprotobuf gRPC::grpc++)
endif()

# 可执行文件
add_executable(my-etcd ${SOURCES})
target_include_directories(my-etcd PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(my-etcd PRIVATE my-etcd-core Threads::Threads)

# 编译选项
if(MSVC)
    target_compile_options(my-etcd PRIVATE /W4 /utf-8)
    target_compile_options(my-etcd PRIVATE $<$<CONFIG:Debug>:/Od /Zi>)
    target_compile_options(my-etcd PRIVATE $<$<CONFIG:Release>:/O2>)
else()
    target_compile_options(my-etcd PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(my-etcd PRIVATE $<$<CONFIG:Debug>:-g -O0>)
    target_compile_options(my-etcd PRIVATE $<$<CONFIG:Release>:-O3>)
endif()

# ========================================
# 单元测试
# ========================================

set(TEST_SOURCES
    tests/cpp/test_main.cpp
    tests/cpp/test_mvcc.cpp
    tests/cpp/test_backend.cpp
    tests/cpp/test_lease.cpp
    tests/cpp/test_watch.cpp
    tests/cpp/test_kvstore.cpp
    tests/cpp/test_raft.cpp
)

add_executable(my-etcd-test ${TEST_SOURCES})

target_include_directories(my-etcd-test PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(my-etcd-test PRIVATE my-etcd-core Threads::Threads)

# 如果找到 gRPC，测试程序也启用
if(Protobuf_FOUND AND gRPC_FOUND)
    target_compile_definitions(my-etcd-test PRIVATE MYETCD_GRPC_SUPPORT)
    target_link_libraries(my-etcd-test PRIVATE protobuf::libprotobuf gRPC::grpc++)
endif()

# 测试目标 - 构建并运行
enable_testing()
add_test(NAME my-etcd-unit-tests COMMAND my-etcd-test)

# 测试编译选项
if(MSVC)
    target_compile_options(my-etcd-test PRIVATE /W4 /utf-8)
    target_compile_options(my-etcd-test PRIVATE $<$<CONFIG:Debug>:/Od /Zi>)
else()
    target_compile_options(my-etcd-test PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(my-etcd-test PRIVATE $<$<CONFIG:Debug>:-g -O0>)
    target_compile_options(my-etcd-test PRIVATE -Wno-unused-parameter)  # 测试中参数可能未使用
endif()