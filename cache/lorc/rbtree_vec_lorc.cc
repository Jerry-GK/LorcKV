#include <algorithm>
#include <cassert>
#include <iomanip>
#include <algorithm>
#include <climits>
#include <shared_mutex>
#include "rocksdb/rbtree_vec_lorc.h"
#include "rocksdb/rbtree_vec_lorc_iter.h"
#include "memory/arena.h"

namespace ROCKSDB_NAMESPACE {

RBTreeSliceVecRangeCache::RBTreeSliceVecRangeCache(size_t capacity_, bool enable_logger_)
    : LogicallyOrderedSliceVecRangeCache(capacity_, enable_logger_), cache_timestamp(0) {
}

RBTreeSliceVecRangeCache::~RBTreeSliceVecRangeCache() {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_); // Lock for destruction
    orderedRanges.clear();
    lengthMap.clear();
}

void RBTreeSliceVecRangeCache::putRange(ReferringSliceVecRange&& newRefRange) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_); // Acquire unique lock for writing
    std::chrono::high_resolution_clock::time_point start_time;
    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }

    std::vector<SliceVecRange> mergedRanges; 
    std::string newRangeStr = newRefRange.toString();

    // Find the first SliceVecRange that may overlap with newRefRange (keys in ref ranges are all user_key)
    auto it = orderedRanges.upper_bound(SliceVecRange(newRefRange.startKey()));
    if (it != orderedRanges.begin()) {
        --it;
    }
    Slice lastStartKey = newRefRange.startKey();
    bool leftIncluded = true;
    while (it != orderedRanges.end() && it->startUserKey() <= newRefRange.endKey()) {
        if (it->endUserKey() < newRefRange.startKey()) {
            // no overlap
            ++it;
            continue;
        }

        // Handle left split: if existing SliceVecRange starts before newRefRange
        // If exists, it must be the first branch matched in the loop
        if (it->startUserKey() < newRefRange.startKey() && it->endUserKey() >= newRefRange.startKey()) {
            lastStartKey = it->endUserKey();
            leftIncluded = false;
        } else if (it->startUserKey() <= newRefRange.endKey()) {
            // overlapping without left split
            // masterialize the newRefRange of (lastStartKey, it->startUserKey())
            assert(lastStartKey <= it->startUserKey());
            if (lastStartKey < it->startUserKey()) {
                SliceVecRange nonOverlappingSubRange = newRefRange.dumpSubRange(lastStartKey, it->startUserKey(), leftIncluded, false);
                if (nonOverlappingSubRange.isValid() && nonOverlappingSubRange.length() > 0) {
                    mergedRanges.emplace_back(std::move(nonOverlappingSubRange));
                }
            }
            lastStartKey = it->endUserKey();
            leftIncluded = false;
        }

        // Remove the old overlapping SliceVecRange from both containers
        auto sliceVecRangeStartKeys = lengthMap.equal_range(it->length());
        for (auto itt = sliceVecRangeStartKeys.first; itt != sliceVecRangeStartKeys.second; ++itt) {
            if (itt->second == it->startUserKey()) {
                lengthMap.erase(itt);
                break;
            }
        }
        this->current_size -= it->byteSize();
        this->total_range_length -= it->length();
        
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

    // Add the new merged SliceVecRange to both containers
    // TODO(jr): figure out concat(merge) or not
    if (this->concatContinuousRanges) {
        // string underlying data of mergedRanges is moved
        SliceVecRange mergedRange = SliceVecRange::concatRangesMoved(mergedRanges);
        lengthMap.emplace(mergedRange.length(), mergedRange.startUserKey().ToString());
        this->current_size += mergedRange.byteSize();
        this->total_range_length += mergedRange.length();
        orderedRanges.emplace(std::move(mergedRange));
    } else {
        for (auto& mergedRange : mergedRanges) {
            lengthMap.emplace(mergedRange.length(), mergedRange.startUserKey().ToString());
            this->current_size += mergedRange.byteSize();
            this->total_range_length += mergedRange.length();
            orderedRanges.emplace(std::move(mergedRange));
        }
    }

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
    logger.debug("Total SliceVecRange length: " + std::to_string(this->total_range_length));
    logger.debug("Total SliceVecRange num: " + std::to_string(this->orderedRanges.size()));
    logger.debug("----------------------------------------");

    if (this->enable_statistic) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        this->cache_statistic.increasePutRangeNum();
        this->cache_statistic.increasePutRangeTotalTime(duration.count());
    }

    // deconstruction of the overlapping strings of old ranges with the deconstruction of leftRanges and rightRanges
}

bool RBTreeSliceVecRangeCache::updateEntry(const Slice& internal_key, const Slice& value) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    // Update the entry in the SliceVecRange
    Slice user_key = Slice(internal_key.data(), internal_key.size() - SliceVecRange::internal_key_extra_bytes);
    auto it = orderedRanges.upper_bound(SliceVecRange(internal_key));
    if (it != orderedRanges.begin()) {
        it--;
    }
    if (it == orderedRanges.end() || it->startUserKey() > user_key || it->endUserKey() < user_key) {
        return false;
    }
    ParsedInternalKey parsed_internal_key;
    Status s = ParseInternalKey(internal_key, &parsed_internal_key, false);
    SequenceNumber seq_num = parsed_internal_key.sequence;
    ValueType type = parsed_internal_key.type;
    return it->update(internal_key, value);
}

void RBTreeSliceVecRangeCache::victim() {    
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    // Evict the shortest SliceVecRange to minimize impact
    if (this->current_size <= this->capacity) {
        return;
    }
    if (lengthMap.empty()) {
        return;
    }
    
    auto victimRangeStartKey = lengthMap.begin()->second;
    // find the SliceVecRange in orderedRanges and remove it
    auto it = orderedRanges.find(SliceVecRange(victimRangeStartKey));
    if (it == orderedRanges.end() || it->startUserKey() != victimRangeStartKey) {
        logger.error("Victim SliceVecRange not found in orderedRanges");
        assert(false);
        return;
    }

    // If multiple ranges exist, remove the victim
    // If only one SliceVecRange remains, truncate it to fit capacity
    if (orderedRanges.size() > 1 || this->capacity <= 0) {
        logger.debug("Victim: " + it->toString());
        this->current_size -= it->byteSize();
        this->total_range_length -= it->length();
        orderedRanges.erase(it);
        lengthMap.erase(lengthMap.begin());
    } else {
        // Truncate the last remaining SliceVecRange
        logger.debug("Truncate: " + it->toString());
        lengthMap.erase(lengthMap.find(it->length()));
        it->truncate(this->capacity);
        lengthMap.emplace(it->length(), it->startUserKey().ToString());
        this->current_size = it->byteSize();
        this->total_range_length = it->length();
    }
}

void RBTreeSliceVecRangeCache::pinRange(std::string startKey) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    auto it = orderedRanges.find(SliceVecRange(startKey));
    if (it != orderedRanges.end() && it->startUserKey() == startKey) {
        // update the timestamp
        it->setTimestamp(this->cache_timestamp++);
    }
}

SliceVecRangeCacheIterator* RBTreeSliceVecRangeCache::newSliceVecRangeCacheIterator(Arena* arena) const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_); 
    auto mem = arena->AllocateAligned(sizeof(RBTreeSliceVecRangeCacheIterator));
    return new (mem) RBTreeSliceVecRangeCacheIterator(this);
}

void RBTreeSliceVecRangeCache::printAllRanges() const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_); // Acquire shared lock for reading
    logger.debug("All ranges in RBTreeSliceVecRangeCache:");
    for (const auto& range : orderedRanges) {
        logger.debug(range.toString());
    }

    logger.debug("All keys in RBTreeSliceVecRangeCache:");
    for (const auto& range : orderedRanges) {
        for (size_t i = 0; i < range.length(); ++i) {
            ParsedInternalKey parsed_key;
            Status s = ParseInternalKey(range.internalKeyAt(i), &parsed_key, false);
            logger.debug("User Key: " + parsed_key.user_key.ToString() + ", Seq = " + std::to_string(parsed_key.sequence) +
                         ", Type = " + std::to_string(static_cast<unsigned char>(parsed_key.type)));
        }
    }
}

}  // namespace ROCKSDB_NAMESPACE
