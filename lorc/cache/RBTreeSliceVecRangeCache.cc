#include <algorithm>
#include <iostream>
#include <cassert>
#include <iomanip>
#include <algorithm>
#include <climits>
#include "RBTreeSliceVecRangeCache.h"
#include "../iterator/RBTreeSliceVecRangeCacheIterator.h"

RBTreeSliceVecRangeCache::RBTreeSliceVecRangeCache(int max_size)
    : LogicallyOrderedSliceVecRangeCache(max_size) {
}

RBTreeSliceVecRangeCache::~RBTreeSliceVecRangeCache() {
    orderedRanges.clear();
    lengthMap.clear();
}

void RBTreeSliceVecRangeCache::putRange(SliceVecRange&& newRange) {
    std::chrono::high_resolution_clock::time_point start_time;
    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }
    
    // Merge strategy: new SliceVecRange data takes priority, old SliceVecRange data keeps non-overlapping parts
    std::vector<SliceVecRange> leftRanges;  // Store the left part of split ranges (at most one)
    std::vector<SliceVecRange> rightRanges; // Store the right part of split ranges (at most one)
    std::string newRangeStr = newRange.toString();

    // Find the first SliceVecRange that may overlap with newRange
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

        // Handle left split: if existing SliceVecRange starts before newRange
        if (it->startKey() < newRange.startKey() && it->endKey() >= newRange.startKey()) {
            int leftSplitIdx = it->find(newRange.startKey()) - 1;
            if (leftSplitIdx >= 0 && leftSplitIdx < it->getSize()) {
                leftRanges.emplace_back(std::move(it->subRangeView(0, leftSplitIdx)));
            }
        }

        // Handle right split: if existing SliceVecRange ends after newRange
        if (it->endKey() > newRange.endKey() && it->startKey() <= newRange.endKey()) {
            int rightSplitIdx = it->find(newRange.endKey());
            if (rightSplitIdx >= 0 && rightSplitIdx < it->getSize() && it->keyAt(rightSplitIdx) == newRange.endKey()) {
                rightSplitIdx++;
            }
            if (rightSplitIdx >= 0 && rightSplitIdx < it->getSize()) {
                rightRanges.emplace_back(std::move(it->subRangeView(rightSplitIdx, it->getSize() - 1)));
            }
        }

        // Remove the old SliceVecRange from both containers
        auto SliceVecRange = lengthMap.equal_range(it->getSize());
        for (auto itt = SliceVecRange.first; itt != SliceVecRange.second; ++itt) {
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
    std::vector<SliceVecRange> mergedRanges;
    mergedRanges.reserve(leftRanges.size() + 1 + rightRanges.size());
    
    // Use emplace_back and std::move to avoid unnecessary copies
    for (SliceVecRange& SliceVecRange : leftRanges) {
        mergedRanges.emplace_back(std::move(SliceVecRange));
    }
    mergedRanges.emplace_back(std::move(newRange));
    for (SliceVecRange& SliceVecRange : rightRanges) {
        mergedRanges.emplace_back(std::move(SliceVecRange));
    }
    
    SliceVecRange mergedRange = SliceVecRange::concatRangesMoved(mergedRanges);
    
    // Add the new merged SliceVecRange to both containers
    lengthMap.emplace(mergedRange.getSize(), mergedRange.startKey().ToString());
    this->current_size += mergedRange.getSize();
    orderedRanges.emplace(std::move(mergedRange));

    // Trigger eviction if cache size exceeds limit
    while (this->current_size > this->max_size) {
        this->victim();
    }

    // Debug output: print all ranges in order
    Logger::debug("----------------------------------------");
    Logger::debug("All ranges after putRange: " + newRangeStr);
    for (auto it = orderedRanges.begin(); it != orderedRanges.end(); ++it) {
        Logger::debug("SliceVecRange: " + it->toString());
    }
    Logger::debug("Total SliceVecRange size: " + std::to_string(this->current_size));
    Logger::debug("----------------------------------------");

    if (this->enable_statistic) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        this->cache_statistic.increasePutRangeNum();
        this->cache_statistic.increasePutRangeTotalTime(duration.count());
    }
}

bool RBTreeSliceVecRangeCache::updateEntry(const Slice& key, const Slice& value) {
    // Update the entry in the SliceVecRange
    auto it = orderedRanges.upper_bound(SliceVecRange(key));
    if (it == orderedRanges.begin()) {
        it--;
    }
    if (it == orderedRanges.end() || it->startKey() > key || it->endKey() < key) {
        return false;
    }
    return it->update(key, value);
}

void RBTreeSliceVecRangeCache::victim() {
    // Evict the shortest SliceVecRange to minimize impact
    if (this->current_size <= this->max_size) {
        return;
    }
    auto victimRangeStartKey = lengthMap.begin()->second;
    // find the SliceVecRange in orderedRanges and remove it
    auto it = orderedRanges.find(SliceVecRange(victimRangeStartKey));
    if (it == orderedRanges.end() || it->startKey() != victimRangeStartKey) {
        Logger::error("Victim SliceVecRange not found in orderedRanges");
        assert(false);
        return;
    }

    // If multiple ranges exist, remove the victim
    // If only one SliceVecRange remains, truncate it to fit max_size
    if (orderedRanges.size() > 1 || this->max_size <= 0) {
        Logger::debug("Victim: " + it->toString());
        this->current_size -= lengthMap.begin()->first;
        orderedRanges.erase(it);    // TODO(jr): background delete
        lengthMap.erase(lengthMap.begin());
    } else {
        // Truncate the last remaining SliceVecRange
        Logger::debug("Truncate: " + it->toString());
        lengthMap.erase(lengthMap.find(it->getSize()));
        it->truncate(this->max_size);
        lengthMap.emplace(it->getSize(), it->startKey().ToString());
        this->current_size = it->getSize();
    }
}

void RBTreeSliceVecRangeCache::pinRange(std::string startKey) {
    auto it = orderedRanges.find(SliceVecRange(startKey));
    if (it != orderedRanges.end() && it->startKey() == startKey) {
        // update the timestamp
        it->setTimestamp(this->cache_timestamp++);
    }
}

SliceVecRangeCacheIterator* RBTreeSliceVecRangeCache::newSliceVecRangeCacheIterator() const {
    return new RBTreeSliceVecRangeCacheIterator(this);
}