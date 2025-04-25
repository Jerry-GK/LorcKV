#include "RBTreeRangeCache.h"
#include <algorithm>
#include <iostream>
#include <cassert>
#include <iomanip>
#include <algorithm>
#include <climits>

RBTreeRangeCache::RBTreeRangeCache(int max_size)
    : LogicallyOrderedRangeCache(max_size) {
}

RBTreeRangeCache::~RBTreeRangeCache() {
    orderedRanges.clear();
    lengthMap.clear();
}

void RBTreeRangeCache::putRange(Range&& newRange) {
    std::chrono::high_resolution_clock::time_point start_time;
    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }
    
    // Merge strategy: new range data takes priority, old range data keeps non-overlapping parts
    std::vector<Range> leftRanges;  // Store the left part of split ranges (at most one)
    std::vector<Range> rightRanges; // Store the right part of split ranges (at most one)
    std::string newRangeStr = newRange.toString();

    // Find the first range that may overlap with newRange
    auto it = orderedRanges.upper_bound(newRange);
    if (it != orderedRanges.begin()) {
        --it;
    }
    while (it != orderedRanges.end() && it->startKey() <= newRange.endKey()) {
        if (it->endKey() < newRange.startKey()) {
            // no overlap
            ++it;
            continue;
        }

        // Handle left split: if existing range starts before newRange
        if (it->startKey() < newRange.startKey() && it->endKey() >= newRange.startKey()) {
            int leftSplitIdx = it->find(newRange.startKey()) - 1;
            if (leftSplitIdx >= 0 && leftSplitIdx < it->getSize()) {
                leftRanges.emplace_back(std::move(it->subRangeView(0, leftSplitIdx)));
            }
        }

        // Handle right split: if existing range ends after newRange
        if (it->endKey() > newRange.endKey() && it->startKey() <= newRange.endKey()) {
            int rightSplitIdx = it->find(newRange.endKey());
            if (rightSplitIdx >= 0 && rightSplitIdx < it->getSize() && it->keyAt(rightSplitIdx) == newRange.endKey()) {
                rightSplitIdx++;
            }
            if (rightSplitIdx >= 0 && rightSplitIdx < it->getSize()) {
                rightRanges.emplace_back(std::move(it->subRangeView(rightSplitIdx, it->getSize() - 1)));
            }
        }

        // Remove the old range from both containers
        auto range = lengthMap.equal_range(it->getSize());
        for (auto itt = range.first; itt != range.second; ++itt) {
            if (itt->second == it->startKey()) {
                lengthMap.erase(itt);
                break;
            }
        }
        this->current_size -= it->getSize();
        it = orderedRanges.erase(it);
    }
    
    // Concatenate: left ranges + newRange + right ranges
    if (leftRanges.size() > 1 || rightRanges.size() > 1) {
        Logger::error("Left or right ranges size is greater than 1");
        return;
    }
    
    // Reserve space to avoid reallocations
    std::vector<Range> mergedRanges;
    mergedRanges.reserve(leftRanges.size() + 1 + rightRanges.size());
    
    // Use emplace_back and std::move to avoid unnecessary copies
    for (Range& range : leftRanges) {
        mergedRanges.emplace_back(std::move(range));
    }
    mergedRanges.emplace_back(std::move(newRange));
    for (Range& range : rightRanges) {
        mergedRanges.emplace_back(std::move(range));
    }
    
    Range mergedRange = Range::concatRangesMoved(mergedRanges);
    
    // Add the new merged range to both containers
    lengthMap.emplace(mergedRange.getSize(), mergedRange.startKey());
    this->current_size += mergedRange.getSize();
    orderedRanges.insert(std::move(mergedRange));

    // Trigger eviction if cache size exceeds limit
    while (this->current_size > this->max_size) {
        this->victim();
    }

    // Debug output: print all ranges in order
    Logger::debug("----------------------------------------");
    Logger::debug("All ranges after putRange and victim: " + newRangeStr);
    for (auto it = orderedRanges.begin(); it != orderedRanges.end(); ++it) {
        Logger::debug("Range: " + it->toString());
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

CacheResult RBTreeRangeCache::getRange(const std::string& start_key, const std::string& end_key) {
    std::chrono::high_resolution_clock::time_point start_time;
    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }
    
    Logger::debug("Get Range: < " + start_key + " -> " + end_key + " >");
    CacheResult result(false, false, Range(false), {});
    std::vector<Range> partial_hit_ranges;

    // Find the first range that might overlap with the query
    auto it = orderedRanges.upper_bound(Range({start_key, start_key}, {}, 1)); // the parameter is a virtual temp range
    if (it != orderedRanges.begin()) {
        --it;
    }
    if (it != orderedRanges.end()) {
        // Check for full hit: when a single range completely contains the query range
        if (it->startKey() <= start_key && it->endKey() >= end_key) {
            // full hit
            full_hit_count++;
            this->pinRange(it->startKey());
            Range fullHitRange = it->subRange(start_key, end_key); // dont use subRangeView to preserve underlying data
            hit_size += fullHitRange.getSize();
            result = CacheResult(true, false, std::move(fullHitRange), {});
        } else {
            // Check for partial hits: when one or more ranges partially overlap with query
            if (it->endKey() < start_key) {
                ++it; // Check the next range if current one ends too early
            }
            if (it!= orderedRanges.end() && it->startKey() <= end_key && it->endKey() >= start_key) {
                // partial hit
                do {
                    // dont use subRangeView to preserve underlying data
                    partial_hit_ranges.emplace_back(std::move(it->subRange(
                        std::max(it->startKey(), start_key),
                        std::min(it->endKey(), end_key))));
                    this->pinRange(it->startKey());
                } while(++it != orderedRanges.end() && it->startKey() <= end_key && it->endKey() >= start_key);
            } else {
                // no hit
                CacheResult result(false, false, Range(false), {});
            }
        }
    }

    // Update statistics
    full_query_count++;
    query_size += std::stoi(end_key) - std::stoi(start_key) + 1;

    // Handle partial hits
    if (!result.isFullHit() && !partial_hit_ranges.empty()) {
        // partial hit
        for (const auto& range : partial_hit_ranges) {
            hit_size += range.getSize();
        }
        // 使用std::move移动整个vector
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

void RBTreeRangeCache::victim() {
    // Evict the shortest range to minimize impact
    if (this->current_size <= this->max_size) {
        return;
    }
    auto victimRangeStartKey = lengthMap.begin()->second;
    // find the range in orderedRanges and remove it
    auto it = orderedRanges.find(Range({victimRangeStartKey, victimRangeStartKey}, {}, 1));
    if (it == orderedRanges.end() || it->startKey() != victimRangeStartKey) {
        Logger::error("Victim range not found in orderedRanges");
        return;
    }

    // If multiple ranges exist, remove the victim
    // If only one range remains, truncate it to fit max_size
    if (orderedRanges.size() > 1 || this->max_size <= 0) {
        Logger::debug("Victim: " + it->toString());
        this->current_size -= lengthMap.begin()->first;
        orderedRanges.erase(it);
        lengthMap.erase(lengthMap.begin());
    } else {
        // Truncate the last remaining range
        Logger::debug("Truncate: " + it->toString());
        lengthMap.erase(lengthMap.find(it->getSize()));
        it->truncate(this->max_size);
        lengthMap.emplace(it->getSize(), it->startKey());
        this->current_size = it->getSize();
    }
}

void RBTreeRangeCache::pinRange(std::string startKey) {
    auto it = orderedRanges.find(Range({startKey, startKey}, {}, 1));
    if (it != orderedRanges.end() && it->startKey() == startKey) {
        // update the timestamp
        it->setTimestamp(this->cache_timestamp++);
    }
}

double RBTreeRangeCache::fullHitRate() const {
    if (full_query_count == 0) {
        return 0;
    }
    return (double)full_hit_count / full_query_count;
}

double RBTreeRangeCache::hitSizeRate() const {
    if (query_size == 0) {
        return 0;
    }
    return (double)hit_size / query_size;
}
