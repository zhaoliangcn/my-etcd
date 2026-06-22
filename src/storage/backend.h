#pragma once

#include "common/types.h"
#include <string>
#include <map>
#include <mutex>
#include <fstream>
#include <optional>

namespace myetcd {

// 后端持久化存储 - 简单的文件存储实现
// 存储格式: [key_len(4)][key][value_len(4)][value]
class Backend {
public:
    explicit Backend(const std::string& data_dir);
    ~Backend();

    Backend(const Backend&) = delete;
    Backend& operator=(const Backend&) = delete;

    // 打开存储
    bool Open();

    // 关闭存储
    void Close();

    // 写入键值
    bool Put(const std::string& key, const std::string& value);

    // 读取键值
    std::optional<std::string> Get(const std::string& key);

    // 删除键值
    bool Delete(const std::string& key);

    // 范围查询 [start, end)
    std::vector<KeyValue> Range(const std::string& start, const std::string& end, int64_t limit = 0);

    // 前缀查询
    std::vector<KeyValue> PrefixRange(const std::string& prefix, int64_t limit = 0);

    // 批量写入
    bool BatchPut(const std::map<std::string, std::string>& kvs);

    // 获取所有 key
    std::vector<std::string> Keys();

    // 同步到磁盘
    bool Sync();

    // 获取数据文件路径
    std::string DataFilePath() const { return data_dir_ + "/backend.db"; }

private:
    bool LoadFromDisk();
    bool FlushToDisk();

    std::string data_dir_;
    std::map<std::string, std::string> store_;
    mutable std::mutex mu_;
};

} // namespace myetcd