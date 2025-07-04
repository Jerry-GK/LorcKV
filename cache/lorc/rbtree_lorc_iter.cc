#include "rocksdb/rbtree_lorc_iter.h"
#include "rocksdb/rbtree_lorc.h"
#include "rocksdb/lorc_iter.h"
#include <shared_mutex>

namespace ROCKSDB_NAMESPACE {

RBTreeLogicalOrderedRangeCacheIterator::~RBTreeLogicalOrderedRangeCacheIterator() {
    
}

RBTreeLogicalOrderedRangeCacheIterator::RBTreeLogicalOrderedRangeCacheIterator(const RBTreeLogicalOrderedRangeCache* cache_)
    : cache(cache_), current_index(-1), iter_status(Status()), valid(false) {}

bool RBTreeLogicalOrderedRangeCacheIterator::Valid() const {
    return valid && iter_status.ok();
}

bool RBTreeLogicalOrderedRangeCacheIterator::HasNextInRange() const {
    if (!valid) {
        return false;
    }
    return (size_t)current_index + 1 < (*current_range)->length();
}

void RBTreeLogicalOrderedRangeCacheIterator::SeekToFirst() {
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

void RBTreeLogicalOrderedRangeCacheIterator::SeekToLast() {
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

void RBTreeLogicalOrderedRangeCacheIterator::Seek(const Slice& target_internal_key) {
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

void RBTreeLogicalOrderedRangeCacheIterator::SeekForPrev(const Slice& target_internal_key) {
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

void RBTreeLogicalOrderedRangeCacheIterator::Next() {
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

void RBTreeLogicalOrderedRangeCacheIterator::Prev() {
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

Slice RBTreeLogicalOrderedRangeCacheIterator::key() const {
    static std::string empty_string;
    if (!valid) {
        return empty_string;
    }
    return (*current_range)->internalKeyAt(current_index);
}

Slice RBTreeLogicalOrderedRangeCacheIterator::userKey() const {
    static std::string empty_string;
    if (!valid) {
        return empty_string;
    }
    return (*current_range)->userKeyAt(current_index);
}

Slice RBTreeLogicalOrderedRangeCacheIterator::value() const {
    static std::string empty_string;
    if (!valid) {
        return empty_string;
    }
    return (*current_range)->valueAt(current_index);
}

Status RBTreeLogicalOrderedRangeCacheIterator::status() const {
    return this->iter_status;
}

}  // namespace ROCKSDB_NAMESPACE
