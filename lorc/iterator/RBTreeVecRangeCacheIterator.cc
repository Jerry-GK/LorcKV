#include "RBTreeVecRangeCacheIterator.h"
#include "../cache/RBTreeVecRangeCache.h"
#include "../range/VecRange.h"

RBTreeVecRangeCacheIterator::~RBTreeVecRangeCacheIterator() {
    
}

RBTreeVecRangeCacheIterator::RBTreeVecRangeCacheIterator(const RBTreeRangeCache* cache)
    : cache(cache), valid(false), current_index(-1) {}

bool RBTreeVecRangeCacheIterator::Valid() const {
    return valid && status_str.empty();
}

bool RBTreeVecRangeCacheIterator::HasNextInRange() const {
    if (!valid) {
        return false;
    }
    return current_index + 1 < current_range->getSize();
}

void RBTreeVecRangeCacheIterator::SeekToFirst() {
    if (!cache) {
        status_str = "Invalid cache reference";
        valid = false;
        return;
    }
    current_range = cache->orderedRanges.begin();
    if (current_range != cache->orderedRanges.end()) {
        current_index = 0;
        valid = true;
    } else {
        status_str = "Cache is empty";
        valid = false;
    }
}

void RBTreeVecRangeCacheIterator::SeekToLast() {
    if (!cache) {
        status_str = "Invalid cache reference";
        valid = false;
        return;
    }
    if (cache->orderedRanges.empty()) {
        status_str = "Cache is empty";
        valid = false;
        return;
    }
    current_range = std::prev(cache->orderedRanges.end());
    current_index = current_range->getSize() - 1;
    valid = true;
}

void RBTreeVecRangeCacheIterator::Seek(const std::string& target) {
    if (!cache) {
        status_str = "Invalid cache reference";
        valid = false;
        return;
    }
    VecRange temp_range({target}, {""}, 1);
    current_range = cache->orderedRanges.upper_bound(temp_range); // startKey > target
    if (current_range != cache->orderedRanges.begin()) {
        --current_range;
    }
    
    if (current_range != cache->orderedRanges.end()) {
        current_index = current_range->find(target);
        if (current_index == -1) {
            ++current_range;
            current_index = 0;
            if (current_range == cache->orderedRanges.end()) {
                status_str = "No valid VecRange for Seek found for target: " + target;
                valid = false;
                return;
            }
        }
        valid = true;
    } else {
        status_str = "Unknown error in Seek: current_range is end()";
        valid = false;
    }
}

void RBTreeVecRangeCacheIterator::SeekForPrev(const std::string& target) {
    if (!cache) {
        status_str = "Invalid cache reference";
        valid = false;
        return;
    }
    VecRange temp_range({target}, {""}, 1);
    current_range = cache->orderedRanges.upper_bound(temp_range);
    if (current_range != cache->orderedRanges.begin()) {
        --current_range;
    }
    
    if (current_range != cache->orderedRanges.end() && current_range->endKey() >= target) {
        current_index = current_range->find(target);
        if (current_index == -1) {
            current_index = current_range->getSize() - 1;
        }
        valid = true;
    } else {
        status_str = "No valid VecRange for SeekForPrev found for target: " + target;
        valid = false;
    }
}

void RBTreeVecRangeCacheIterator::Next() {
    if (!valid) {
        return;
    }
    
    if (current_index < current_range->getSize() - 1) {
        current_index++;
    } else {
        ++current_range;
        if (current_range != cache->orderedRanges.end()) {
            current_index = 0;
        } else {
            status_str = "Next reached end of cache";
            valid = false;
        }
    }
}

void RBTreeVecRangeCacheIterator::Prev() {
    if (!valid) {
        return;
    }
    
    if (current_index > 0) {
        current_index--;
    } else {
        if (current_range == cache->orderedRanges.begin()) {
            status_str = "Prev reached beginning of cache";
            valid = false;
        } else {
            --current_range;
            current_index = current_range->getSize() - 1;
        }
    }
}

const std::string& RBTreeVecRangeCacheIterator::key() const {
    static std::string empty_string;
    if (!valid) {
        return empty_string;
    }
    return current_range->keyAt(current_index);
}

const std::string& RBTreeVecRangeCacheIterator::value() const {
    static std::string empty_string;
    if (!valid) {
        return empty_string;
    }
    return current_range->valueAt(current_index);
}

std::string RBTreeVecRangeCacheIterator::status() const {
    return this->status_str;
}
