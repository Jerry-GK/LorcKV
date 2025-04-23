#pragma once

#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include "Range.h"

class CacheResult {
public:
    // 接收右值的构造函数
    CacheResult(bool full_hit, bool partial_hit, Range&& full_hit_range,
                const std::vector<Range>& partial_hit_ranges)
        : full_hit(full_hit), partial_hit(partial_hit),
          full_hit_range(std::make_unique<Range>(std::move(full_hit_range))), 
          partial_hit_ranges(partial_hit_ranges) {}

    // 接收左值的构造函数
    CacheResult(bool full_hit, bool partial_hit, const Range& full_hit_range,
                const std::vector<Range>& partial_hit_ranges)
        : full_hit(full_hit), partial_hit(partial_hit),
          full_hit_range(std::make_unique<Range>(full_hit_range)), 
          partial_hit_ranges(partial_hit_ranges) {}

    // 移动构造函数
    CacheResult(CacheResult&& other) noexcept
        : full_hit(other.full_hit),
          partial_hit(other.partial_hit),
          full_hit_range(std::move(other.full_hit_range)),
          partial_hit_ranges(std::move(other.partial_hit_ranges)) {}

    // 移动赋值运算符
    CacheResult& operator=(CacheResult&& other) noexcept {
        if (this != &other) {
            full_hit = other.full_hit;
            partial_hit = other.partial_hit;
            full_hit_range = std::move(other.full_hit_range);
            partial_hit_ranges = std::move(other.partial_hit_ranges);
        }
        return *this;
    }

    // 禁用拷贝构造和拷贝赋值
    CacheResult(const CacheResult&) = delete;
    CacheResult& operator=(const CacheResult&) = delete;

    bool isFullHit() const { return full_hit; }
    bool isPartialHit() const { return partial_hit; }
    const Range& getFullHitRange() const { return *full_hit_range; }
    const std::vector<Range>& getPartialHitRanges() const { return partial_hit_ranges; }
private:
    bool full_hit;
    bool partial_hit;
    std::unique_ptr<Range> full_hit_range;
    std::vector<Range> partial_hit_ranges;
};

// This is a cache for ranges. Key-Value Ranges is the smallest unit of data that can be cached.
class LogicallyOrderedRangeCache {
public:
    LogicallyOrderedRangeCache(int max_size);
    virtual ~LogicallyOrderedRangeCache();

    // insert a key-value range into the cache, update overlapped exsisting ranges
    virtual void putRange(const Range& range) = 0;

    // lookup a key-value range in the cache
    virtual CacheResult getRange(const std::string& start_key, const std::string& end_key) = 0;

    // victim
    virtual void victim() = 0;

    virtual double fullHitRate() const = 0;

    virtual double hitSizeRate() const = 0;

protected:
    int max_size;
    int current_size;
    int full_hit_count;
    int full_query_count;
    int hit_size;
    int query_size;
};