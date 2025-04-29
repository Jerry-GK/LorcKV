#include <algorithm>
#include <iostream>
#include <cassert>
#include <iomanip>
#include "SegmentedVecRangeCache.h"

SegmentedVecRangeCache::SegmentedVecRangeCache(int max_size)
    : LogicallyOrderedVecRangeCache(max_size) {
    entries = std::vector<SegmentedRangeCacheEntry>();
    cache_timestamp = 0;
}

SegmentedVecRangeCache::~SegmentedVecRangeCache() {
    entries.clear();
}

void SegmentedVecRangeCache::putRange(VecRange&& newRange) {
    std::chrono::high_resolution_clock::time_point start_time;
    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }

    std::vector<VecRange> overlappingRanges;
    std::string newRangeStr = newRange.toString();

    // Find all ranges that overlap with the new VecRange
    auto it = entries.begin();
    while (it != entries.end()) {
        bool isOverlap = (newRange.endKey() >= it->getRange().startKey()) && 
                         (newRange.startKey() <= it->getRange().endKey());
        if (isOverlap) {
            // Update merged VecRange boundaries
            overlappingRanges.emplace_back(it->getRange());
            this->current_size -= it->getRange().getSize();
            it = entries.erase(it); // Remove old VecRange
        } else {
            ++it;
        }
    }

    // Merge data: new VecRange data takes priority, preserve non-overlapping parts of old ranges
    std::vector<std::string>& mergedKeys = newRange.getKeys();
    std::vector<std::string>& mergedValues = newRange.getValues();

    std::vector<std::string> oldLeftKeys;
    std::vector<std::string> oldLeftValues;
    std::vector<std::string> oldRightKeys;
    std::vector<std::string> oldRightValues;
    
    // Add non-overlapping data from old ranges
    for (const auto& oldRange : overlappingRanges) {
        // Process left non-overlapping part (startKey < newRange.startKey())
        if (oldRange.startKey() < newRange.startKey()) {
            size_t splitIdx = 0;
            while (splitIdx < oldRange.getSize() && oldRange.keyAt(splitIdx) < newRange.startKey()) {
                oldLeftKeys.emplace_back(std::move(oldRange.keyAt(splitIdx)));
                oldLeftValues.emplace_back(std::move(oldRange.valueAt(splitIdx)));
                splitIdx++;
            }
        }

        // Process right non-overlapping part (endKey > newRange.endKey())
        if (oldRange.endKey() > newRange.endKey()) {
            size_t splitIdx = oldRange.getSize() - 1;
            while (splitIdx >= 0 && oldRange.keyAt(splitIdx) > newRange.endKey()) {
                oldRightKeys.emplace_back(std::move(oldRange.keyAt(splitIdx)));
                oldRightValues.emplace_back(std::move(oldRange.valueAt(splitIdx)));
                splitIdx--;
            }
            // reverse the order of oldRightKeys and oldRightValues
            std::reverse(oldRightKeys.begin(), oldRightKeys.end());
            std::reverse(oldRightValues.begin(), oldRightValues.end());
        }
    }

    // push front
    mergedKeys.insert(mergedKeys.begin(), oldLeftKeys.begin(), oldLeftKeys.end());
    mergedValues.insert(mergedValues.begin(), oldLeftValues.begin(), oldLeftValues.end());
    // push back
    mergedKeys.insert(mergedKeys.end(), oldRightKeys.begin(), oldRightKeys.end());
    mergedValues.insert(mergedValues.end(), oldRightValues.begin(), oldRightValues.end());

    // 按键排序合并后的数据
    // std::sort(mergedKeys.begin(), mergedKeys.end());
    // auto last = std::unique(mergedKeys.begin(), mergedKeys.end());
    // mergedKeys.erase(last, mergedKeys.end());

    // 创建合并后的新Range并插入缓存
    VecRange mergedRange(std::move(mergedKeys), std::move(mergedValues), mergedKeys.size());
    entries.emplace_back(SegmentedRangeCacheEntry(mergedRange, this->cache_timestamp++));

    // Keep ranges ordered by startKey
    std::sort(entries.begin(), entries.end(), 
        [](const SegmentedRangeCacheEntry& a, const SegmentedRangeCacheEntry& b) {
            return a.getRange().startKey() < b.getRange().startKey();
        });

    this->current_size += mergedRange.getSize();

    // victim
    while (this->current_size > this->max_size) {
        this->victim();
    }

    // Debug output: print all ranges' startKey and endKey in order
    Logger::debug("----------------------------------------");
    Logger::debug("All ranges after putRange and victim: " + newRangeStr);
    for (int i = 0; i < this->entries.size(); i++) {
        const VecRange& VecRange = this->entries[i].getRange();
        Logger::debug("VecRange[" + std::to_string(i+1) + "]: " + VecRange.toString());
    }
    Logger::debug("Total VecRange size: " + std::to_string(this->current_size));
    Logger::debug("----------------------------------------");

    if (this->enable_statistic) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        this->cache_statistic.increasePutRangeNum();
        this->cache_statistic.increasePutRangeTotalTime(duration.count());
    }
}

CacheResult SegmentedVecRangeCache::getRange(const std::string& start_key, const std::string& end_key) {
    std::chrono::high_resolution_clock::time_point start_time;
    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }

    Logger::debug("Get VecRange: < " + start_key + " -> " + end_key + " >");
    CacheResult result(false, false, VecRange(false), {});
    std::vector<VecRange> partial_hit_ranges;
    for (size_t i = 0; i < this->entries.size(); i++) {
        if (entries[i].getRange().startKey() <= start_key && entries[i].getRange().endKey() >= end_key) {
            // full hit
            increaseFullHitCount();
            this->pinRange(i);
            VecRange fullHitRange = entries[i].getRange().subRange(start_key, end_key);
            increaseHitSize(fullHitRange.getSize());
            result = CacheResult(true, false, std::move(fullHitRange), {});
            break;
        } else if (entries[i].getRange().startKey() <= end_key && entries[i].getRange().endKey() >= start_key) {
            // partial hit
            partial_hit_ranges.emplace_back(entries[i].getRange().subRange(
                std::max(entries[i].getRange().startKey(), start_key),
                std::min(entries[i].getRange().endKey(), end_key)));
            this->pinRange(i); 
        }
    }

    increaseFullQueryCount();
    increaseQuerySize(std::stoi(end_key) - std::stoi(start_key) + 1);    // TODO(jr): delete this code

    if (!result.isFullHit() && !partial_hit_ranges.empty()) {
        // partial hit
        for (const auto& VecRange : partial_hit_ranges) {
            increaseHitSize(VecRange.getSize());
        }
        result = CacheResult(false, true, VecRange(false), std::move(partial_hit_ranges));
    } 

    if (this->enable_statistic) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        this->cache_statistic.increaseGetRangeNum();
        this->cache_statistic.increaseGetRangeTotalTime(duration.count());
    }

    return result;
}

// void SegmentedVecRangeCache::victim() {
//     // victim a random VecRange
//     if (this->current_size <= this->max_size) {
//         return;
//     }
//     int randomIndex = rand() % entries.size();
//     if (entries.size() == 1) {
//         Logger::debug("Truncate: " + entries[0].getRange().toString());
//         VecRange truncatedRange = entries[0].getRange().subRange(0, this->max_size - 1);
//         entries.erase(entries.begin());
//         entries.emplace_back(SegmentedRangeCacheEntry(truncatedRange, this->cache_timestamp++));
//         this->current_size = truncatedRange.getSize();
//     } else {
//         Logger::debug("Victim: " + entries[randomIndex].getRange().toString());
//         this->current_size -= entries[randomIndex].getRange().getSize();
//         entries.erase(entries.begin() + randomIndex);
//     }
// }

// void SegmentedVecRangeCache::victim() {
//     // victim the LRU VecRange
//     if (this->current_size <= this->max_size) {
//         return;
//     }
//     auto minRangeIt = std::min_element(entries.begin(), entries.end(),
//         [](const SegmentedRangeCacheEntry& a, const SegmentedRangeCacheEntry& b) {
//             return a.getTimestamp() < b.getTimestamp();
//         });
//         if (minRangeIt != entries.end()) {
//             // only truncate if there is only one VecRange left (seldom triggered)
//             if (entries.size() == 1) {
//                 Logger::debug("Truncate: " + minRangeIt->getRange().toString());
//                 VecRange truncatedRange = minRangeIt->getRange().subRange(0, this->max_size - 1);
//                 entries.erase(minRangeIt);
//                 entries.emplace_back(SegmentedRangeCacheEntry(truncatedRange, this->cache_timestamp++));
//                 this->current_size = truncatedRange.getSize();
//             } else {
//                 Logger::debug("Victim: " + minRangeIt->getRange().toString());
//                 this->current_size -= minRangeIt->getRange().getSize();
//                 entries.erase(minRangeIt);
//             }
//     } else {
//         Logger::debug("No VecRange to victim");
//     }
// }

void SegmentedVecRangeCache::victim() {
    // Remove the shortest VecRange to make space
    if (this->current_size <= this->max_size) {
        return;
    }
    
    // Find the VecRange with minimum size
    auto minRangeIt = std::min_element(entries.begin(), entries.end(),
        [](const SegmentedRangeCacheEntry& a, const SegmentedRangeCacheEntry& b) {
            return a.getRange().getSize() < b.getRange().getSize();
        });
    
    if (minRangeIt != entries.end()) {
        // If there are multiple ranges, remove the shortest one
        if (entries.size() > 1) {
            Logger::debug("Victim: " + minRangeIt->getRange().toString());
            this->current_size -= minRangeIt->getRange().getSize();
            entries.erase(minRangeIt);
        } else {
            // If only one VecRange exists, truncate it
            Logger::debug("Truncate: " + minRangeIt->getRange().toString());
            minRangeIt->getRange().truncate(this->max_size);
            this->pinRange(0);
            this->current_size = minRangeIt->getRange().getSize();
        }
    } else {
        Logger::debug("No VecRange to victim");
    }
}

// void SegmentedVecRangeCache::victim() {
//     // victim the longest VecRange
//     if (this->current_size <= this->max_size) {
//         return;
//     }
//     auto minRangeIt = std::min_element(entries.begin(), entries.end(),
//         [](const SegmentedRangeCacheEntry& a, const SegmentedRangeCacheEntry& b) {
//             return a.getRange().getSize() > b.getRange().getSize();
//         });
//     if (minRangeIt != entries.end()) {
//         // only truncate if there is only one VecRange left (seldom triggered)
//         if (entries.size() == 1) {
//             Logger::debug("Truncate: " + minRangeIt->getRange().toString());
//             VecRange truncatedRange = minRangeIt->getRange().subRange(0, this->max_size - 1);
//             entries.erase(minRangeIt);
//             entries.emplace_back(SegmentedRangeCacheEntry(truncatedRange, this->cache_timestamp++));
//             this->current_size = truncatedRange.getSize();
//         } else {
//             Logger::debug("Victim: " + minRangeIt->getRange().toString());
//             this->current_size -= minRangeIt->getRange().getSize();
//             entries.erase(minRangeIt);
//         }
//     } else {
//         Logger::debug("No VecRange to victim");
//     }
// }

void SegmentedVecRangeCache::pinRange(int index) {
    assert(index >= 0 && index < entries.size());
    entries[index].timestamp = this->cache_timestamp++;
}
