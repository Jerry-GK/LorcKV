#include "RBTreeRangeCacheIterator.h"
#include "../RBTreeRangeCache.h"
#include "../Range.h"

RBTreeRangeCacheIterator::RBTreeRangeCacheIterator(const RBTreeRangeCache* cache)
    : cache(cache), valid(false), current_index(-1) {}

bool RBTreeRangeCacheIterator::Valid() const {
    return valid && error_status.empty();
}

void RBTreeRangeCacheIterator::SeekToFirst() {
    if (!cache) {
        error_status = "Invalid cache reference";
        valid = false;
        return;
    }
    current = cache->orderedRanges.begin();
    if (current != cache->orderedRanges.end()) {
        current_range = current;
        current_index = 0;
        valid = true;
    } else {
        valid = false;
    }
}

void RBTreeRangeCacheIterator::SeekToLast() {
    if (!cache) {
        error_status = "Invalid cache reference";
        valid = false;
        return;
    }
    if (cache->orderedRanges.empty()) {
        valid = false;
        return;
    }
    current = std::prev(cache->orderedRanges.end());
    current_range = current;
    current_index = current->getSize() - 1;
    valid = true;
}

void RBTreeRangeCacheIterator::Seek(const std::string& target) {
    if (!cache) {
        error_status = "Invalid cache reference";
        valid = false;
        return;
    }
    Range temp_range({target, target}, {}, 1);
    current = cache->orderedRanges.lower_bound(temp_range);
    
    if (current != cache->orderedRanges.end()) {
        current_range = current;
        current_index = current->find(target);
        if (current_index == -1) {
            current_index = 0;
        }
        valid = true;
    } else {
        valid = false;
    }
}

void RBTreeRangeCacheIterator::SeekForPrev(const std::string& target) {
    if (!cache) {
        error_status = "Invalid cache reference";
        valid = false;
        return;
    }
    Range temp_range({target, target}, {}, 1);
    current = cache->orderedRanges.upper_bound(temp_range);
    if (current != cache->orderedRanges.begin()) {
        --current;
    }
    
    if (current != cache->orderedRanges.end() && current->endKey() >= target) {
        current_range = current;
        current_index = current->find(target);
        if (current_index == -1) {
            current_index = current->getSize() - 1;
        }
        valid = true;
    } else {
        valid = false;
    }
}

void RBTreeRangeCacheIterator::Next() {
    if (!valid) {
        return;
    }
    
    if (current_index < current_range->getSize() - 1) {
        current_index++;
    } else {
        ++current;
        if (current != cache->orderedRanges.end()) {
            current_range = current;
            current_index = 0;
        } else {
            valid = false;
        }
    }
}

void RBTreeRangeCacheIterator::Prev() {
    if (!valid) {
        return;
    }
    
    if (current_index > 0) {
        current_index--;
    } else {
        if (current == cache->orderedRanges.begin()) {
            valid = false;
        } else {
            --current;
            current_range = current;
            current_index = current->getSize() - 1;
        }
    }
}

std::string RBTreeRangeCacheIterator::key() const {
    if (!valid) {
        return "";
    }
    return current_range->keyAt(current_index);
}

std::string RBTreeRangeCacheIterator::value() const {
    if (!valid) {
        return "";
    }
    return current_range->valueAt(current_index);
}

std::string RBTreeRangeCacheIterator::status() const {
    return error_status;
}
