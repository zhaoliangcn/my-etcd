#include "server/http_handler.h"
#include <sstream>
#include <algorithm>

namespace myetcd {

void HttpResponse::SetJson(const std::string& json) {
    headers["Content-Type"] = "application/json";
    body = json;
}

void HttpResponse::SetError(int code, const std::string& message) {
    status_code = code;
    headers["Content-Type"] = "application/json";
    body = "{\"error\":\"" + json::Escape(message) + "\",\"code\":" + std::to_string(code) + "}";
}

namespace json {

std::string Escape(const std::string& s) {
    std::string result;
    result.reserve(s.size() + 2);
    for (char c : s) {
        switch (c) {
        case '"': result += "\\\""; break;
        case '\\': result += "\\\\"; break;
        case '\n': result += "\\n"; break;
        case '\r': result += "\\r"; break;
        case '\t': result += "\\t"; break;
        default: result += c; break;
        }
    }
    return result;
}

std::string Stringify(const std::map<std::string, std::string>& obj) {
    std::ostringstream oss;
    oss << "{";
    bool first = true;
    for (const auto& [k, v] : obj) {
        if (!first) oss << ",";
        first = false;
        oss << "\"" << Escape(k) << "\":\"" << Escape(v) << "\"";
    }
    oss << "}";
    return oss.str();
}

std::string Stringify(const std::vector<std::map<std::string, std::string>>& arr) {
    std::ostringstream oss;
    oss << "[";
    bool first = true;
    for (const auto& obj : arr) {
        if (!first) oss << ",";
        first = false;
        oss << Stringify(obj);
    }
    oss << "]";
    return oss.str();
}

std::string KvToJson(const KeyValue& kv) {
    std::ostringstream oss;
    oss << "{";
    oss << "\"key\":\"" << Escape(kv.key) << "\",";
    oss << "\"value\":\"" << Escape(kv.value) << "\",";
    oss << "\"create_revision\":" << kv.create_revision << ",";
    oss << "\"mod_revision\":" << kv.mod_revision << ",";
    oss << "\"version\":" << kv.version << ",";
    oss << "\"lease\":" << kv.lease_id;
    oss << "}";
    return oss.str();
}

std::string KvListToJson(const std::vector<KeyValue>& kvs, int64_t count) {
    std::ostringstream oss;
    oss << "{";
    if (count > 0) {
        oss << "\"count\":" << count << ",";
    } else {
        oss << "\"count\":" << kvs.size() << ",";
    }
    oss << "\"kvs\":[";
    bool first = true;
    for (const auto& kv : kvs) {
        if (!first) oss << ",";
        first = false;
        oss << KvToJson(kv);
    }
    oss << "]}";
    return oss.str();
}

std::string WatchEventToJson(const WatchEvent& ev) {
    std::ostringstream oss;
    oss << "{";
    oss << "\"type\":\"" << (ev.type == EventType::PUT ? "PUT" : "DELETE") << "\",";
    oss << "\"kv\":" << KvToJson(ev.kv);
    oss << "}";
    return oss.str();
}

std::string LeaseToJson(LeaseId id, int64_t ttl_ms) {
    std::ostringstream oss;
    oss << "{";
    oss << "\"ID\":" << id << ",";
    oss << "\"TTL\":" << (ttl_ms / 1000);
    oss << "}";
    return oss.str();
}

} // namespace json
} // namespace myetcd