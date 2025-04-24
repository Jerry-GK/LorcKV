#include "SegmentedRangeCache.h"
#include <algorithm>
#include <iostream>
#include <cassert>
#include <iomanip>

SegmentedRangeCache::SegmentedRangeCache(int max_size)
    : LogicallyOrderedRangeCache(max_size) {
    entries = std::vector<SegmentedRangeCacheEntry>();
    cache_timestamp = 0;
}

SegmentedRangeCache::~SegmentedRangeCache() {
    entries.clear();
}

void SegmentedRangeCache::putRange(Range&& newRange) {
    std::chrono::high_resolution_clock::time_point start_time;
    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }

    std::vector<Range> overlappingRanges;
    std::string newRangeStr = newRange.toString();

    // Find all ranges that overlap with the new range
    auto it = entries.begin();
    while (it != entries.end()) {
        bool isOverlap = (newRange.endKey() >= it->getRange().startKey()) && 
                         (newRange.startKey() <= it->getRange().endKey());
        if (isOverlap) {
            // Update merged range boundaries
            overlappingRanges.emplace_back(it->getRange());
            this->current_size -= it->getRange().getSize();
            it = entries.erase(it); // Remove old range
        } else {
            ++it;
        }
    }

    // Merge data: new range data takes priority, preserve non-overlapping parts of old ranges
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

    // ��������ϲ��������
    // std::sort(mergedKeys.begin(), mergedKeys.end());
    // auto last = std::unique(mergedKeys.begin(), mergedKeys.end());
    // mergedKeys.erase(last, mergedKeys.end());

    // �����ϲ������Range�����뻺��
    Range mergedRange(std::move(mergedKeys), std::move(mergedValues), mergedKeys.size());
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
        const Range& range = this->entries[i].getRange();
        Logger::debug("Range[" + std::to_string(i+1) + "]: " + range.toString());
    }
    Logger::debug("Total range size: " + std::to_string(this->current_size));
    Logger::debug("----------------------------------------");

    if (this->enable_statistic) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        this->cache_statistic.putRangeNum++;
        this->cache_statistic.putRangeTotalTime += duration.count();
    }
}

CacheResult SegmentedRangeCache::getRange(const std::string& start_key, const std::string& end_key) {
    std::chrono::high_resolution_clock::time_point start_time;
    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }

    Logger::debug("Get Range: < " + start_key + " -> " + end_key + " >");
    CacheResult result(false, false, Range(false), {});
    std::vector<Range> partial_hit_ranges;
    for (size_t i = 0; i < this->entries.size(); i++) {
        if (entries[i].getRange().startKey() <= start_key && entries[i].getRange().endKey() >= end_key) {
            // full hit
            full_hit_count++;
            this->pinRange(i);
            Range fullHitRange = entries[i].getRange().subRange(start_key, end_key);
            hit_size += fullHitRange.getSize();
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

    full_query_count++;
    query_size += std::stoi(end_key) - std::stoi(start_key) + 1;    // TODO(jr): delete this code

    if (!result.isFullHit() && !partial_hit_ranges.empty()) {
        // partial hit
        for (const auto& range : partial_hit_ranges) {
            hit_size += range.getSize();
        }
        result = CacheResult(false, true, Range(false), std::move(partial_hit_ranges));
    } 

    if (this->enable_statistic) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        this->cache_statistic.getRangeNum++;
        this->cache_statistic.getRangeTotalTime += duration.count();
    }

    return result;
}

// void SegmentedRangeCache::victim() {
//     // victim a random range
//     if (this->current_size <= this->max_size) {
//         return;
//     }
//     int randomIndex = rand() % entries.size();
//     if (entries.size() == 1) {
//         Logger::debug("Truncate: " + entries[0].getRange().toString());
//         Range truncatedRange = entries[0].getRange().subRange(0, this->max_size - 1);
//         entries.erase(entries.begin());
//         entries.emplace_back(SegmentedRangeCacheEntry(truncatedRange, this->cache_timestamp++));
//         this->current_size = truncatedRange.getSize();
//     } else {
//         Logger::debug("Victim: " + entries[randomIndex].getRange().toString());
//         this->current_size -= entries[randomIndex].getRange().getSize();
//         entries.erase(entries.begin() + randomIndex);
//     }
// }

// void SegmentedRangeCache::victim() {
//     // victim the LRU range
//     if (this->current_size <= this->max_size) {
//         return;
//     }
//     auto minRangeIt = std::min_element(entries.begin(), entries.end(),
//         [](const SegmentedRangeCacheEntry& a, const SegmentedRangeCacheEntry& b) {
//             return a.getTimestamp() < b.getTimestamp();
//         });
//         if (minRangeIt != entries.end()) {
//             // only truncate if there is only one range left (seldom triggered)
//             if (entries.size() == 1) {
//                 Logger::debug("Truncate: " + minRangeIt->getRange().toString());
//                 Range truncatedRange = minRangeIt->getRange().subRange(0, this->max_size - 1);
//                 entries.erase(minRangeIt);
//                 entries.emplace_back(SegmentedRangeCacheEntry(truncatedRange, this->cache_timestamp++));
//                 this->current_size = truncatedRange.getSize();
//             } else {
//                 Logger::debug("Victim: " + minRangeIt->getRange().toString());
//                 this->current_size -= minRangeIt->getRange().getSize();
//                 entries.erase(minRangeIt);
//             }
//     } else {
//         Logger::debug("No range to victim");
//     }
// }

void SegmentedRangeCache::victim() {
    // Remove the shortest range to make space
    if (this->current_size <= this->max_size) {
        return;
    }
    
    // Find the range with minimum size
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
            // If only one range exists, truncate it
            Logger::debug("Truncate: " + minRangeIt->getRange().toString());
            minRangeIt->getRange().truncate(this->max_size);
            this->pinRange(0);
            this->current_size = minRangeIt->getRange().getSize();
        }
    } else {
        Logger::debug("No range to victim");
    }
}

// void SegmentedRangeCache::victim() {
//     // victim the longest range
//     if (this->current_size <= this->max_size) {
//         return;
//     }
//     auto minRangeIt = std::min_element(entries.begin(), entries.end(),
//         [](const SegmentedRangeCacheEntry& a, const SegmentedRangeCacheEntry& b) {
//             return a.getRange().getSize() > b.getRange().getSize();
//         });
//     if (minRangeIt != entries.end()) {
//         // only truncate if there is only one range left (seldom triggered)
//         if (entries.size() == 1) {
//             Logger::debug("Truncate: " + minRangeIt->getRange().toString());
//             Range truncatedRange = minRangeIt->getRange().subRange(0, this->max_size - 1);
//             entries.erase(minRangeIt);
//             entries.emplace_back(SegmentedRangeCacheEntry(truncatedRange, this->cache_timestamp++));
//             this->current_size = truncatedRange.getSize();
//         } else {
//             Logger::debug("Victim: " + minRangeIt->getRange().toString());
//             this->current_size -= minRangeIt->getRange().getSize();
//             entries.erase(minRangeIt);
//         }
//     } else {
//         Logger::debug("No range to victim");
//     }
// }

void SegmentedRangeCache::pinRange(int index) {
    assert(index >= 0 && index < entries.size());
    entries[index].timestamp = this->cache_timestamp++;
}

double SegmentedRangeCache::fullHitRate() const {
    if (full_query_count == 0) {
        return 0;
    }
    return (double)full_hit_count / full_query_count;
}

double SegmentedRangeCache::hitSizeRate() const {
    if (query_size == 0) {
        return 0;
    }
    return (double)hit_size / query_size;
}