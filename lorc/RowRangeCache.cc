#include "RowRangeCache.h"
#include <algorithm>
#include <iostream>
#include <cassert>
#include <iomanip>
#include <algorithm>
#include <climits>

RowRangeCache::RowRangeCache(int max_size)
    : LogicallyOrderedRangeCache(max_size) {
}

RowRangeCache::~RowRangeCache() {
    row_map.clear();
}

void RowRangeCache::putRange(Range&& newRange) {
    std::string newRangeStr = newRange.toString();
    // put all key-value pair in newRange into row_map, no copy
    for (int i = 0; i < newRange.getSize(); i++) {
        // move to avoid string copy
        row_map[newRange.keyAt(i)] = std::move(newRange.valueAt(i));
    }
    this->current_size += newRange.getSize();

    while (this->current_size > this->max_size) {
        this->victim();
    }

    // Debug output: print all ranges in order
    Logger::debug("----------------------------------------");
    Logger::debug("All ranges after putRange and victim: " + newRangeStr);
    Logger::debug("Total range size: " + std::to_string(this->current_size));
    Logger::debug("----------------------------------------");
}

CacheResult RowRangeCache::getRange(const std::string& start_key, const std::string& end_key) {
    Logger::debug("Get Range: < " + start_key + " -> " + end_key + " >");
    CacheResult result(false, false, Range(false), {});
    std::vector<std::string> keys;
    std::vector<std::string> values;
    bool hit = false;

    auto it = row_map.lower_bound(start_key);
    if (it != row_map.end() && it->first <= end_key) {
        // for row cache, regard this overlap situation as (full) hit
        hit = true;
        while(it != row_map.end() && it->first <= end_key) {
            // check if the key is in the range
            keys.emplace_back(it->first);
            values.emplace_back(it->second);
            it++;
        }
    } 

    Range full_hit_range(std::move(keys), std::move(values), keys.size());

    if(hit) {
        full_hit_count++;
        hit_size += full_hit_range.getSize();
        result = CacheResult(true, false, std::move(full_hit_range), {});
    }

    // Update statistics
    full_query_count++;
    query_size += std::stoi(end_key) - std::stoi(start_key) + 1;

    // no partial hit for RowRangeCache

    return result;
}

void RowRangeCache::victim() {
    // Evict the shortest range to minimize impact
    if (this->current_size <= this->max_size) {
        return;
    }
    // resize row_map to max_size
    int victim_size = this->current_size - this->max_size;
    auto it = row_map.begin();
    while (it != row_map.end() && victim_size > 0) {
        // erase the first element
        it = row_map.erase(it);
        victim_size--;
    }
    this->current_size = row_map.size();
}

double RowRangeCache::fullHitRate() const {
    if (full_query_count == 0) {
        return 0;
    }
    return (double)full_hit_count / full_query_count;
}

double RowRangeCache::hitSizeRate() const {
    if (query_size == 0) {
        return 0;
    }
    return (double)hit_size / query_size;
}
