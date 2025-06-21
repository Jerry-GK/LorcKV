#include "rocksdb/rbtree_lorc_iter.h"
#include "rocksdb/rbtree_lorc.h"
#include "rocksdb/lorc_iter.h"
#include <shared_mutex>

namespace ROCKSDB_NAMESPACE {

RBTreeLogicallyOrderedRangeCacheIterator::~RBTreeLogicallyOrderedRangeCacheIterator() {
    
}

RBTreeLogicallyOrderedRangeCacheIterator::RBTreeLogicallyOrderedRangeCacheIterator(const RBTreeLogicallyOrderedRangeCache* cache_)
    : cache(cache_), current_index(-1), iter_status(Status()), valid(false) {}

bool RBTreeLogicallyOrderedRangeCacheIterator::Valid() const {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    return valid && iter_status.ok();
}

bool RBTreeLogicallyOrderedRangeCacheIterator::HasNextInRange() const {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!valid) {
        return false;
    }
    return (size_t)current_index + 1 < (*current_range)->length();
}

void RBTreeLogicallyOrderedRangeCacheIterator::SeekToFirst() {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!cache) {
        valid = false;
        return;
    }
    current_range = cache->ordered_physical_ranges.begin();
    if (current_range != cache->ordered_physical_ranges.end()) {
        current_index = 0;
        valid = true;
    } else {
        valid = false;
    }
}

void RBTreeLogicallyOrderedRangeCacheIterator::SeekToLast() {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!cache) {
        valid = false;
        return;
    }
    if (cache->ordered_physical_ranges.empty()) {
        valid = false;
        return;
    }
    current_range = std::prev(cache->ordered_physical_ranges.end());
    current_index = (*current_range)->length() - 1;
    valid = true;
}

void RBTreeLogicallyOrderedRangeCacheIterator::Seek(const Slice& target_internal_key) {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!cache) {
        valid = false;
        return;
    }
    assert(target_internal_key.size() > PhysicalRange::internal_key_extra_bytes);
    Slice target_user_key = Slice(target_internal_key.data(), target_internal_key.size() - PhysicalRange::internal_key_extra_bytes);
    current_range = cache->ordered_physical_ranges.upper_bound(target_user_key); // startKey > target
    if (current_range != cache->ordered_physical_ranges.begin()) {
        --current_range;
    }
    
    if (current_range != cache->ordered_physical_ranges.end()) {
        current_index = (*current_range)->find(target_user_key);
        if (current_index == -1) {
            ++current_range;
            current_index = 0;
            if (current_range == cache->ordered_physical_ranges.end()) {
                valid = false;
                return;
            }
        }
        valid = true;
    } else {
        valid = false;
    }
}

void RBTreeLogicallyOrderedRangeCacheIterator::SeekForPrev(const Slice& target_internal_key) {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!cache) {
        valid = false;
        return;
    }
    assert(target_internal_key.size() > PhysicalRange::internal_key_extra_bytes);
    Slice target_user_key = Slice(target_internal_key.data(), target_internal_key.size() - PhysicalRange::internal_key_extra_bytes);
    current_range = cache->ordered_physical_ranges.upper_bound(target_user_key);
    if (current_range != cache->ordered_physical_ranges.begin()) {
        --current_range;
    }
    
    if (current_range != cache->ordered_physical_ranges.end() && (*current_range)->endUserKey() >= target_user_key) {
        current_index = (*current_range)->find(target_user_key);
        if (current_index == -1) {
            current_index = (*current_range)->length() - 1;
        }
        valid = true;
    } else {
        valid = false;
    }
}

void RBTreeLogicallyOrderedRangeCacheIterator::Next() {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!valid) {
        return;
    }
    
    if ((size_t)current_index < (*current_range)->length() - 1) {
        current_index++;
    } else {
        ++current_range;
        if (current_range != cache->ordered_physical_ranges.end()) {
            current_index = 0;
        } else {
            valid = false;
        }
    }
}

void RBTreeLogicallyOrderedRangeCacheIterator::Prev() {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    if (!valid) {
        return;
    }
    
    if (current_index > 0) {
        current_index--;
    } else {
        if (current_range == cache->ordered_physical_ranges.begin()) {
            valid = false;
        } else {
            --current_range;
            current_index = (*current_range)->length() - 1;
        }
    }
}

Slice RBTreeLogicallyOrderedRangeCacheIterator::key() const {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    static std::string empty_string;
    if (!valid) {
        return empty_string;
    }
    return (*current_range)->internalKeyAt(current_index);
}

Slice RBTreeLogicallyOrderedRangeCacheIterator::userKey() const {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    static std::string empty_string;
    if (!valid) {
        return empty_string;
    }
    return (*current_range)->userKeyAt(current_index);
}

Slice RBTreeLogicallyOrderedRangeCacheIterator::value() const {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    static std::string empty_string;
    if (!valid) {
        return empty_string;
    }
    return (*current_range)->valueAt(current_index);
}

Status RBTreeLogicallyOrderedRangeCacheIterator::status() const {
    std::shared_lock<std::shared_mutex> lock(cache->cache_mutex_);
    return this->iter_status;
}

}  // namespace ROCKSDB_NAMESPACE
