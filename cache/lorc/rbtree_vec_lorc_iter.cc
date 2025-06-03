#include "rocksdb/rbtree_vec_lorc_iter.h"
#include "rocksdb/rbtree_vec_lorc.h"
#include "rocksdb/vec_lorc_iter.h"
#include <shared_mutex>

namespace ROCKSDB_NAMESPACE {

RBTreeSliceVecRangeCacheIterator::~RBTreeSliceVecRangeCacheIterator() {
    
}

RBTreeSliceVecRangeCacheIterator::RBTreeSliceVecRangeCacheIterator(const RBTreeSliceVecRangeCache* cache_)
    : cache(cache_), current_index(-1), iter_status(Status()), valid(false) {}

bool RBTreeSliceVecRangeCacheIterator::Valid() const {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    return valid && iter_status.ok();
}

bool RBTreeSliceVecRangeCacheIterator::HasNextInRange() const {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!valid) {
        return false;
    }
    return (size_t)current_index + 1 < current_range->length();
}

void RBTreeSliceVecRangeCacheIterator::SeekToFirst() {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!cache) {
        valid = false;
        return;
    }
    current_range = cache->orderedRanges.begin();
    if (current_range != cache->orderedRanges.end()) {
        current_index = 0;
        valid = true;
    } else {
        valid = false;
    }
}

void RBTreeSliceVecRangeCacheIterator::SeekToLast() {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!cache) {
        valid = false;
        return;
    }
    if (cache->orderedRanges.empty()) {
        valid = false;
        return;
    }
    current_range = std::prev(cache->orderedRanges.end());
    current_index = current_range->length() - 1;
    valid = true;
}

void RBTreeSliceVecRangeCacheIterator::Seek(const Slice& target_internal_key) {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!cache) {
        valid = false;
        return;
    }
    SliceVecRange temp_range(target_internal_key);
    current_range = cache->orderedRanges.upper_bound(temp_range); // startKey > target
    if (current_range != cache->orderedRanges.begin()) {
        --current_range;
    }
    
    if (current_range != cache->orderedRanges.end()) {
        current_index = current_range->find(target_internal_key);
        if (current_index == -1) {
            ++current_range;
            current_index = 0;
            if (current_range == cache->orderedRanges.end()) {
                valid = false;
                return;
            }
        }
        valid = true;
    } else {
        valid = false;
    }
}

void RBTreeSliceVecRangeCacheIterator::SeekForPrev(const Slice& target_internal_key) {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!cache) {
        valid = false;
        return;
    }
    SliceVecRange temp_range(target_internal_key);
    current_range = cache->orderedRanges.upper_bound(temp_range);
    if (current_range != cache->orderedRanges.begin()) {
        --current_range;
    }
    
    if (current_range != cache->orderedRanges.end() && current_range->endInternalKey() >= target_internal_key) {
        current_index = current_range->find(target_internal_key);
        if (current_index == -1) {
            current_index = current_range->length() - 1;
        }
        valid = true;
    } else {
        valid = false;
    }
}

void RBTreeSliceVecRangeCacheIterator::Next() {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!valid) {
        return;
    }
    
    if ((size_t)current_index < current_range->length() - 1) {
        current_index++;
    } else {
        ++current_range;
        if (current_range != cache->orderedRanges.end()) {
            current_index = 0;
        } else {
            valid = false;
        }
    }
}

void RBTreeSliceVecRangeCacheIterator::Prev() {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!valid) {
        return;
    }
    
    if (current_index > 0) {
        current_index--;
    } else {
        if (current_range == cache->orderedRanges.begin()) {
            valid = false;
        } else {
            --current_range;
            current_index = current_range->length() - 1;
        }
    }
}

Slice RBTreeSliceVecRangeCacheIterator::key() const {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    static std::string empty_string;
    if (!valid) {
        return empty_string;
    }
    return current_range->internalKeyAt(current_index);
}

Slice RBTreeSliceVecRangeCacheIterator::value() const {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    static std::string empty_string;
    if (!valid) {
        return empty_string;
    }
    return current_range->valueAt(current_index);
}

Status RBTreeSliceVecRangeCacheIterator::status() const {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    return this->iter_status;
}

}  // namespace ROCKSDB_NAMESPACE
