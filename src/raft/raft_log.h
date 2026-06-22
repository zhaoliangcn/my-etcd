#pragma once

#include "common/types.h"
#include <vector>
#include <mutex>
#include <algorithm>

namespace myetcd {

// Raft 日志管理 (内存中的日志)
class RaftLog {
public:
    RaftLog() = default;

    // 获取最后一条日志的索引
    Index LastIndex() const {
        std::lock_guard<std::mutex> lock(mu_);
        if (entries_.empty()) return kNoIndex;
        return entries_.back().index;
    }

    // 获取最后一条日志的任期
    Term LastTerm() const {
        std::lock_guard<std::mutex> lock(mu_);
        if (entries_.empty()) return kNoTerm;
        return entries_.back().term;
    }

    // 获取指定索引的日志任期
    Term TermAt(Index idx) const {
        std::lock_guard<std::mutex> lock(mu_);
        if (idx < first_index_) return kNoTerm;
        size_t offset = idx - first_index_;
        if (offset >= entries_.size()) return kNoTerm;
        return entries_[offset].term;
    }

    // 追加日志条目
    Index Append(const RaftEntry& entry) {
        std::lock_guard<std::mutex> lock(mu_);
        entries_.push_back(entry);
        return entry.index;
    }

    // 追加多条日志
    void Append(const std::vector<RaftEntry>& entries) {
        std::lock_guard<std::mutex> lock(mu_);
        entries_.insert(entries_.end(), entries.begin(), entries.end());
    }

    // 获取从 start 开始的所有日志
    std::vector<RaftEntry> EntriesFrom(Index start) const {
        std::lock_guard<std::mutex> lock(mu_);
        if (start < first_index_) start = first_index_;
        if (start > LastIndexUnsafe()) return {};
        size_t offset = start - first_index_;
        return std::vector<RaftEntry>(entries_.begin() + offset, entries_.end());
    }

    // 获取 [start, end) 范围的日志
    std::vector<RaftEntry> Slice(Index start, Index end) const {
        std::lock_guard<std::mutex> lock(mu_);
        if (start < first_index_) start = first_index_;
        if (end > LastIndexUnsafe() + 1) end = LastIndexUnsafe() + 1;
        if (start >= end) return {};
        size_t s = start - first_index_;
        size_t e = end - first_index_;
        return std::vector<RaftEntry>(entries_.begin() + s, entries_.begin() + e);
    }

    // 从指定索引截断 (用于日志压缩)
    void TruncateFrom(Index idx) {
        std::lock_guard<std::mutex> lock(mu_);
        if (idx <= first_index_) return;
        size_t offset = idx - first_index_;
        if (offset >= entries_.size()) {
            entries_.clear();
            first_index_ = idx;
            return;
        }
        entries_.erase(entries_.begin(), entries_.begin() + offset);
        first_index_ = idx;
    }

    // 从指定索引开始删除后续日志 (用于日志冲突)
    void TruncateTo(Index idx) {
        std::lock_guard<std::mutex> lock(mu_);
        if (idx < first_index_) {
            entries_.clear();
            return;
        }
        size_t keep = idx - first_index_ + 1;
        if (keep >= entries_.size()) return;
        entries_.resize(keep);
    }

    // 按索引获取日志条目
    std::optional<RaftEntry> EntryAt(Index idx) const {
        std::lock_guard<std::mutex> lock(mu_);
        if (idx < first_index_) return std::nullopt;
        size_t offset = idx - first_index_;
        if (offset >= entries_.size()) return std::nullopt;
        return entries_[offset];
    }

    // 获取日志数量
    size_t Size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return entries_.size();
    }

    // 获取第一个日志索引
    Index FirstIndex() const {
        std::lock_guard<std::mutex> lock(mu_);
        return first_index_;
    }

    // 检查是否匹配
    bool MatchTerm(Index idx, Term term) const {
        return TermAt(idx) == term;
    }

    // 获取所有日志 (用于快照)
    std::vector<RaftEntry> AllEntries() const {
        std::lock_guard<std::mutex> lock(mu_);
        return entries_;
    }

    // 重置
    void Reset(Index start_index) {
        std::lock_guard<std::mutex> lock(mu_);
        entries_.clear();
        first_index_ = start_index;
    }

    // 从快照恢复
    void Restore(Index last_index) {
        std::lock_guard<std::mutex> lock(mu_);
        entries_.clear();
        first_index_ = last_index + 1;
    }

private:
    Index LastIndexUnsafe() const {
        if (entries_.empty()) return first_index_ > 0 ? first_index_ - 1 : kNoIndex;
        return entries_.back().index;
    }

    mutable std::mutex mu_;
    std::vector<RaftEntry> entries_;
    Index first_index_ = 1;
};

} // namespace myetcd