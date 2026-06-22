#pragma once

#include "common/types.h"
#include <string>
#include <map>
#include <functional>
#include <sstream>

namespace myetcd {

// 简单的 HTTP 请求/响应
struct HttpRequest {
    std::string method;
    std::string path;
    std::map<std::string, std::string> headers;
    std::string body;
    std::map<std::string, std::string> query_params;
};

struct HttpResponse {
    int status_code = 200;
    std::map<std::string, std::string> headers;
    std::string body;

    void SetJson(const std::string& json);
    void SetError(int code, const std::string& message);
};

// HTTP 请求处理器
using HttpHandler = std::function<HttpResponse(const HttpRequest&)>;

// JSON 工具函数
namespace json {

std::string Escape(const std::string& s);
std::string Stringify(const std::map<std::string, std::string>& obj);
std::string Stringify(const std::vector<std::map<std::string, std::string>>& arr);

// 构建 KV 响应 JSON
std::string KvToJson(const KeyValue& kv);
std::string KvListToJson(const std::vector<KeyValue>& kvs, int64_t count = 0);
std::string WatchEventToJson(const WatchEvent& ev);
std::string LeaseToJson(LeaseId id, int64_t ttl_ms);

} // namespace json

} // namespace myetcd