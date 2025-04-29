#include <algorithm>
#include <iostream>
#include <cassert>
#include <iomanip>
#include <algorithm>
#include <climits>
#include "RBTreeContRangeCache.h"
#include "../iterator/RBTreeContRangeCacheIterator.h"

RBTreeContRangeCache::RBTreeContRangeCache(int max_size)
    : LogicallyOrderedContRangeCache(max_size) {
}

RBTreeContRangeCache::~RBTreeContRangeCache() {
    orderedRanges.clear();
    lengthMap.clear();
}

void RBTreeContRangeCache::putRange(ContRange&& newRange) {
    std::chrono::high_resolution_clock::time_point start_time;
    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }
    
    // Merge strategy: new ContRange data takes priority, old ContRange data keeps non-overlapping parts
    std::vector<ContRange> leftRanges;  // Store the left part of split ranges (at most one)
    std::vector<ContRange> rightRanges; // Store the right part of split ranges (at most one)
    std::string newRangeStr = newRange.toString();

    // Find the first ContRange that may overlap with newRange
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

        // Handle left split: if existing ContRange starts before newRange
        if (it->startKey() < newRange.startKey() && it->endKey() >= newRange.startKey()) {
            int leftSplitIdx = it->find(newRange.startKey()) - 1;
            if (leftSplitIdx >= 0 && leftSplitIdx < it->getSize()) {
                leftRanges.emplace_back(std::move(it->subRangeView(0, leftSplitIdx)));
            }
        }

        // Handle right split: if existing ContRange ends after newRange
        if (it->endKey() > newRange.endKey() && it->startKey() <= newRange.endKey()) {
            int rightSplitIdx = it->find(newRange.endKey());
            if (rightSplitIdx >= 0 && rightSplitIdx < it->getSize() && it->keyAt(rightSplitIdx) == newRange.endKey()) {
                rightSplitIdx++;
            }
            if (rightSplitIdx >= 0 && rightSplitIdx < it->getSize()) {
                rightRanges.emplace_back(std::move(it->subRangeView(rightSplitIdx, it->getSize() - 1)));
            }
        }

        // Remove the old ContRange from both containers
        auto ContRange = lengthMap.equal_range(it->getSize());
        for (auto itt = ContRange.first; itt != ContRange.second; ++itt) {
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
    std::vector<ContRange> mergedRanges;
    mergedRanges.reserve(leftRanges.size() + 1 + rightRanges.size());
    
    // Use emplace_back and std::move to avoid unnecessary copies
    for (ContRange& ContRange : leftRanges) {
        mergedRanges.emplace_back(std::move(ContRange));
    }
    mergedRanges.emplace_back(std::move(newRange));
    for (ContRange& ContRange : rightRanges) {
        mergedRanges.emplace_back(std::move(ContRange));
    }
    
    ContRange mergedRange = ContRange::concatRangesMoved(mergedRanges);
    
    // Add the new merged ContRange to both containers
    lengthMap.emplace(mergedRange.getSize(), mergedRange.startKey().ToString());
    this->current_size += mergedRange.getSize();
    orderedRanges.emplace(std::move(mergedRange));
    // assert(res.second); // Ensure insertion was successful

    // Trigger eviction if cache size exceeds limit
    while (this->current_size > this->max_size) {
        this->victim();
    }

    // Debug output: print all ranges in order
    Logger::debug("----------------------------------------");
    Logger::debug("All ranges after putRange: " + newRangeStr);
    for (auto it = orderedRanges.begin(); it != orderedRanges.end(); ++it) {
        Logger::debug("ContRange: " + it->toString());
    }
    Logger::debug("Total ContRange size: " + std::to_string(this->current_size));
    Logger::debug("----------------------------------------");

    if (this->enable_statistic) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        this->cache_statistic.increasePutRangeNum();
        this->cache_statistic.increasePutRangeTotalTime(duration.count());
    }
}

void RBTreeContRangeCache::victim() {
    // Evict the shortest ContRange to minimize impact
    if (this->current_size <= this->max_size) {
        return;
    }
    auto victimRangeStartKey = lengthMap.begin()->second;
    // find the ContRange in orderedRanges and remove it
    auto it = orderedRanges.find(ContRange({victimRangeStartKey}, {""}, 1));
    if (it == orderedRanges.end() || it->startKey() != victimRangeStartKey) {
        Logger::error("Victim ContRange not found in orderedRanges");
        assert(false);
        return;
    }

    // If multiple ranges exist, remove the victim
    // If only one ContRange remains, truncate it to fit max_size
    if (orderedRanges.size() > 1 || this->max_size <= 0) {
        Logger::debug("Victim: " + it->toString());
        this->current_size -= lengthMap.begin()->first;
        orderedRanges.erase(it);
        lengthMap.erase(lengthMap.begin());
    } else {
        // Truncate the last remaining ContRange
        Logger::debug("Truncate: " + it->toString());
        lengthMap.erase(lengthMap.find(it->getSize()));
        it->truncate(this->max_size);
        lengthMap.emplace(it->getSize(), it->startKey().ToString());
        this->current_size = it->getSize();
    }
}

void RBTreeContRangeCache::pinRange(std::string startKey) {
    auto it = orderedRanges.find(ContRange({startKey}, {""}, 1));
    if (it != orderedRanges.end() && it->startKey() == startKey) {
        // update the timestamp
        it->setTimestamp(this->cache_timestamp++);
    }
}

ContRangeCacheIterator* RBTreeContRangeCache::newContRangeCacheIterator() const {
    return new RBTreeContRangeCacheIterator(this);
}