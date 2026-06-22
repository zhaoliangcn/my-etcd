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

# 可执行文件
add_executable(my-etcd ${SOURCES})

target_include_directories(my-etcd PRIVATE ${CMAKE_SOURCE_DIR}/src)

target_link_libraries(my-etcd PRIVATE Threads::Threads)

# 如果找到 gRPC，则启用 gRPC 支持
if(Protobuf_FOUND AND gRPC_FOUND)
    target_compile_definitions(my-etcd PRIVATE MYETCD_GRPC_SUPPORT)
    target_link_libraries(my-etcd PRIVATE protobuf::libprotobuf gRPC::grpc++)
    message(STATUS "gRPC support enabled")
else()
    message(STATUS "gRPC not found, using HTTP-only mode")
endif()

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