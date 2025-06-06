#pragma once

#include <string>
#include <vector>
#include <memory>
#include <algorithm>
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

/**
 * @brief LogicalRange class represents a logical range of keys in a key-value store.
 * It has no underlying data storage.
 */
class LogicalRange {
private:
    std::string start_user_key;   // start user key, non-empty
    std::string end_user_key;     // end user key, empty if not sure
    size_t range_length;    // length of the range in bytes. zero if it's ended by end_key
    bool in_range_cache;

public:
    LogicalRange(const std::string& startUserKey, const std::string& endUserKey, size_t length, bool inRangeCache) {
        start_user_key = startUserKey;
        end_user_key = endUserKey;
        range_length = length;
        in_range_cache = inRangeCache;
    }

    std::string startUserKey() const {
        return start_user_key;
    }

    std::string endUserKey() const {
        return end_user_key;
    }

    size_t length() const {
        return range_length;
    }

    bool isInRangeCache() const {
        return in_range_cache;
    }

    std::string toString() const {
        std::string endUserKeyStr = end_user_key.empty() ? "(undetermined)" : end_user_key;
        std::string str = "< " + this->startUserKey() + " -> " + endUserKeyStr + " >"
            + " ( len = " + std::to_string(this->length()) + " )";
        return str;
    }
};

/**
 * @brief LogicalRangesView class manages an ordered array of logical ranges.
 */
class LogicalRangesView {
private:
    std::vector<LogicalRange> logical_ranges;

public:
    LogicalRangesView() = default;

    void putRange(const LogicalRange& range, bool left_concat, bool right_concat) {
        auto it = std::lower_bound(logical_ranges.begin(), logical_ranges.end(), range,
                                  [](const LogicalRange& a, const LogicalRange& b) {
                                      return a.startUserKey() < b.startUserKey();
                                  });
        
        size_t insert_pos = it - logical_ranges.begin();
        
        // ¼ì²é×ó²àºÏ²¢
        LogicalRange merged_range = range;
        size_t left_remove_start = insert_pos;
        
        if (left_concat && insert_pos > 0) {
            const auto& left_range = logical_ranges[insert_pos - 1];
            std::string left_end = left_range.startUserKey();
            
            if (left_range.startUserKey() <= merged_range.startUserKey()) {
                merged_range = LogicalRange(
                    left_range.startUserKey(),
                    merged_range.endUserKey(),
                    left_range.length() + merged_range.length(),
                    left_range.isInRangeCache() || merged_range.isInRangeCache()
                );
                left_remove_start = insert_pos - 1;
            }
        }
        
        size_t right_remove_end = insert_pos;
        
        if (right_concat && insert_pos < logical_ranges.size()) {
            const auto& right_range = logical_ranges[insert_pos];
            if (merged_range.startUserKey() <= right_range.startUserKey()) {
                merged_range = LogicalRange(
                    merged_range.startUserKey(),
                    right_range.endUserKey(),
                    merged_range.length() + right_range.length(),
                    merged_range.isInRangeCache() || right_range.isInRangeCache()
                );
                right_remove_end = insert_pos + 1;
            }
        }
        
        if (left_remove_start < right_remove_end) {
            logical_ranges.erase(logical_ranges.begin() + left_remove_start,
                               logical_ranges.begin() + right_remove_end);
            insert_pos = left_remove_start;
        }
        
        logical_ranges.insert(logical_ranges.begin() + insert_pos, merged_range);
    }

    void removeRange(const std::string& startUserKey) {
        logical_ranges.erase(
            std::remove_if(logical_ranges.begin(), logical_ranges.end(),
                           [&startUserKey](const LogicalRange& range) {
                               return range.startUserKey() == startUserKey;
                           }),
            logical_ranges.end());
    }

    const std::vector<LogicalRange>& getLogicalRanges() const {
        return logical_ranges;
    }

    size_t size() const {
        return logical_ranges.size();
    }

    bool empty() const {
        return logical_ranges.empty();
    }

    void clear() {
        logical_ranges.clear();
    }
};

}  // namespace ROCKSDB_NAMESPACE
