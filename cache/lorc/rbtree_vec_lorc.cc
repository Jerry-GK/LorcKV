#include <algorithm>
#include <cassert>
#include <iomanip>
#include <algorithm>
#include <climits>
#include "rocksdb/rbtree_vec_lorc.h"
#include "rocksdb/rbtree_vec_lorc_iter.h"
#include "memory/arena.h"

namespace ROCKSDB_NAMESPACE {

RBTreeSliceVecRangeCache::RBTreeSliceVecRangeCache(size_t capacity_)
    : LogicallyOrderedSliceVecRangeCache(capacity_) {
}

RBTreeSliceVecRangeCache::~RBTreeSliceVecRangeCache() {
    orderedRanges.clear();
    lengthMap.clear();
}

void RBTreeSliceVecRangeCache::putRange(ReferringSliceVecRange&& newRefRange) {
    std::chrono::high_resolution_clock::time_point start_time;
    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }
    
    std::vector<SliceVecRange> mergedRanges; 
    std::string newRangeStr = newRefRange.toString();

    // Find the first SliceVecRange that may overlap with newRefRange
    auto it = orderedRanges.upper_bound(SliceVecRange(newRefRange.startKey()));
    if (it != orderedRanges.begin()) {
        --it;
    }
    Slice lastStartKey = newRefRange.startKey();
    bool leftIncluded = true;
    while (it != orderedRanges.end() && it->startKey() <= newRefRange.endKey()) {
        if (it->endKey() < newRefRange.startKey()) {
            // no overlap
            ++it;
            continue;
        }

        // Handle left split: if existing SliceVecRange starts before newRefRange
        // If exists, it must be the first branch matched in the loop
        if (it->startKey() < newRefRange.startKey() && it->endKey() >= newRefRange.startKey()) {
            lastStartKey = it->endKey();
            leftIncluded = false;
        } else if (it->startKey() <= newRefRange.endKey()) {
            // overlapping without left split
            // masterialize the newRefRange of (lastStartKey, it->startKey())
            assert(lastStartKey <= it->startKey());
            if (lastStartKey < it->startKey()) {
                SliceVecRange nonOverlappingSubRange = newRefRange.dumpSubRange(lastStartKey, it->startKey(), leftIncluded, false);
                if (nonOverlappingSubRange.isValid() && nonOverlappingSubRange.length() > 0) {
                    mergedRanges.emplace_back(std::move(nonOverlappingSubRange));
                }
            }
            lastStartKey = it->endKey();
            leftIncluded = false;
        }

        // Remove the old overlapping SliceVecRange from both containers
        auto sliceVecRangeStartKeys = lengthMap.equal_range(it->length());
        for (auto itt = sliceVecRangeStartKeys.first; itt != sliceVecRangeStartKeys.second; ++itt) {
            if (itt->second == it->startKey()) {
                lengthMap.erase(itt);
                break;
            }
        }
        this->current_size -= it->byteSize();
        
        // Move the old SliceVecRange in range cache to mergedRanges, and remove it (later merged as one large range into the range cache)
        // Using extract() without data copy
        auto current = it++;
        auto node = orderedRanges.extract(current);
        mergedRanges.emplace_back(std::move(node.value()));
    }

    // handle the last non-overlapping subrange if exists (case that has no right split)
    if (leftIncluded ? lastStartKey <= newRefRange.endKey() : lastStartKey < newRefRange.endKey()) {
        SliceVecRange nonOverlappingSubRange = newRefRange.dumpSubRange(lastStartKey, newRefRange.endKey(), leftIncluded, true);
        if (nonOverlappingSubRange.isValid() && nonOverlappingSubRange.length() > 0) {
            mergedRanges.emplace_back(std::move(nonOverlappingSubRange));
        }
    }
    
    // print mergedRanges
    for (auto& range : mergedRanges) {
        logger.debug("Merged SliceVecRange: " + range.toString());
    }
    SliceVecRange mergedRange = SliceVecRange::concatRangesMoved(mergedRanges);
    // string underlying data of mergedRanges is moved
    
    // Add the new merged SliceVecRange to both containers
    lengthMap.emplace(mergedRange.length(), mergedRange.startKey().ToString());
    this->current_size += mergedRange.byteSize();
    orderedRanges.emplace(std::move(mergedRange));

    // Trigger eviction if cache size exceeds limit
    while (this->current_size > this->capacity) {
        this->victim();
    }

    // Debug output: print all ranges in order
    logger.debug("----------------------------------------");
    logger.debug("All ranges after putRange: " + newRangeStr);
    for (auto itt = orderedRanges.begin(); itt != orderedRanges.end(); ++itt) {
        logger.debug("SliceVecRange: " + itt->toString());
    }
    logger.debug("Total SliceVecRange size: " + std::to_string(this->current_size));
    logger.debug("----------------------------------------");

    if (this->enable_statistic) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        this->cache_statistic.increasePutRangeNum();
        this->cache_statistic.increasePutRangeTotalTime(duration.count());
    }

    // deconstruction of the overlapping strings of old ranges with the deconstruction of leftRanges and rightRanges
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
    if (this->current_size <= this->capacity) {
        return;
    }
    auto victimRangeStartKey = lengthMap.begin()->second;
    // find the SliceVecRange in orderedRanges and remove it
    auto it = orderedRanges.find(SliceVecRange(victimRangeStartKey));
    if (it == orderedRanges.end() || it->startKey() != victimRangeStartKey) {
        logger.error("Victim SliceVecRange not found in orderedRanges");
        assert(false);
        return;
    }

    // If multiple ranges exist, remove the victim
    // If only one SliceVecRange remains, truncate it to fit capacity
    if (orderedRanges.size() > 1 || this->capacity <= 0) {
        logger.debug("Victim: " + it->toString());
        orderedRanges.erase(it);
        this->current_size -= it->byteSize();
        lengthMap.erase(lengthMap.begin());
    } else {
        // Truncate the last remaining SliceVecRange
        logger.debug("Truncate: " + it->toString());
        lengthMap.erase(lengthMap.find(it->length()));
        it->truncate(this->capacity);
        lengthMap.emplace(it->length(), it->startKey().ToString());
        this->current_size = it->byteSize();
    }
}

void RBTreeSliceVecRangeCache::pinRange(std::string startKey) {
    auto it = orderedRanges.find(SliceVecRange(startKey));
    if (it != orderedRanges.end() && it->startKey() == startKey) {
        // update the timestamp
        it->setTimestamp(this->cache_timestamp++);
    }
}

SliceVecRangeCacheIterator* RBTreeSliceVecRangeCache::newSliceVecRangeCacheIterator(Arena* arena) const {
    auto mem = arena->AllocateAligned(sizeof(RBTreeSliceVecRangeCacheIterator));
    return new (mem) RBTreeSliceVecRangeCacheIterator(this);
}

}  // namespace ROCKSDB_NAMESPACE
