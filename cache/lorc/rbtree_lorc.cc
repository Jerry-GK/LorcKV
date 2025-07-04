#include <algorithm>
#include <cassert>
#include <iomanip>
#include <algorithm>
#include <climits>
#include <shared_mutex>
#include "rocksdb/rbtree_lorc.h"
#include "rocksdb/rbtree_lorc_iter.h"
#include "memory/arena.h"

namespace ROCKSDB_NAMESPACE {

RBTreeLogicalOrderedRangeCache::RBTreeLogicalOrderedRangeCache(size_t capacity_, LorcLogger::Level logger_level_, PhysicalRangeType physical_range_type_)
    : LogicalOrderedRangeCache(capacity_, logger_level_, physical_range_type_), cache_timestamp(0) {
}

RBTreeLogicalOrderedRangeCache::~RBTreeLogicalOrderedRangeCache() {
    lockWrite();
    ordered_physical_ranges.clear();
    physical_range_length_map.clear();
    ranges_view.clear();
    unlockWrite();
}

void RBTreeLogicalOrderedRangeCache::putGapPhysicalRange(ReferringRange&& newRefRange, bool leftConcat, bool rightConcat, bool emptyConcat, std::string emptyConcatLeftKey, std::string emptyConcatRightKey) {
    lockWrite();
    std::chrono::high_resolution_clock::time_point start_time;

    std::unique_ptr<PhysicalRange> newRange;
    if (LogicalOrderedRangeCache::getPhysicalRangeType() == PhysicalRangeType::CONTINUOUS) {
        newRange = ContinuousPhysicalRange::buildFromReferringRange(newRefRange);
    } else if (LogicalOrderedRangeCache::getPhysicalRangeType() == PhysicalRangeType::VEC) {
        newRange = VecPhysicalRange::buildFromReferringRange(newRefRange);
    } else {
        logger.error("Unsupported PhysicalRangeType for RBTreeLogicalOrderedRangeCache");
        unlockWrite();
        return;
    }

    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }

    if (!emptyConcat) {
        assert(emptyConcatLeftKey.empty() && emptyConcatRightKey.empty());

        // Update the logical ranges view
        ranges_view.putLogicalRange(LogicalRange(newRange->startUserKey().ToString(), newRange->endUserKey().ToString(), newRange->length(), true, true, true), leftConcat, rightConcat);

        // Put into physical ranges directly since no overlapping
        physical_range_length_map.emplace(newRange->length(), newRange->startUserKey().ToString());
        this->current_size += newRange->byteSize();
        this->total_range_length += newRange->length();
        ordered_physical_ranges.emplace(std::move(newRange));
    } else {
        // empty actual range only for concat adjacent ranges
        assert(leftConcat && rightConcat && !emptyConcatLeftKey.empty() && !emptyConcatRightKey.empty());
        ranges_view.putLogicalRange(LogicalRange(emptyConcatLeftKey, emptyConcatRightKey, 0, true, true, true), true, true);
        // no need to change actual physical ranges info
    }

        // Update the cache sequence number after putting a new range
    this->setRangeCacheSeqNum(std::max(this->getRangeCacheSeqNum(), newRefRange.getSeqNum()));

    // do NOT do victim here (call victim externally after filling all gap ranges)
    // while (this->current_size > this->capacity) {
    //     this->victim();
    // }

    // logger
    logger.debug("\n----------------------------------------");
    this->printAllLogicalRanges();
    logger.debug("----------------------------------------");
    // this->printAllPhysicalRanges();
    // logger.debug("----------------------------------------\n");

    if (this->enable_statistic) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        this->cache_statistic.increasePutRangeNum();
        this->cache_statistic.increasePutRangeTotalTime(duration.count());
    }
    unlockWrite();
}

bool RBTreeLogicalOrderedRangeCache::updateEntry(const Slice& internal_key, const Slice& value) {
    // update Entry is done with outside write lock
    Slice user_key = Slice(internal_key.data(), internal_key.size() - PhysicalRange::internal_key_extra_bytes);

    // Find the logical range that may contain the user key
    auto logical_ranges = ranges_view.getLogicalRanges();
    auto find_first_range = [&logical_ranges](const Slice& key) {
        auto it = std::lower_bound(logical_ranges.begin(), logical_ranges.end(), key,
            [](const LogicalRange& range, const Slice& key_) {
                return range.endUserKey() < key_;
            });
        return it;
    };
    auto range_it = find_first_range(user_key);
    if (range_it == logical_ranges.end() || range_it->startUserKey() > user_key) {
        // The user key is not in any logical range, no need to update!
        return false;
    }
    Slice logical_range_start_key = range_it->startUserKey();
    Slice logical_range_end_key = range_it->endUserKey();
    if (!(logical_range_start_key <= user_key && user_key <= logical_range_end_key)) {
        logger.error("User key " + user_key.ToString() + " is not in the logical range: " + range_it->toString());
        assert(false);
        return false;
    }

    // Update/Insert the entry in the PhysicalRange
    auto it = ordered_physical_ranges.upper_bound(internal_key);
    if (it != ordered_physical_ranges.begin()) {
        it--;
    }
    if (it == ordered_physical_ranges.end() || (*it)->startUserKey() > user_key) { // (*it)->endUserKey() < user_key is possible in physical range
        logger.error("User key " + user_key.ToString() + " found in logical range but not found in any physical range");
        assert(false);
        return false;
    }
    assert((*it)->startUserKey() <= user_key);
    if ((*it)->endUserKey() < user_key && (*it)->endUserKey() == logical_range_end_key) {
        // The user key is not in the PhysicalRange, do not update
        logger.error("User key " + user_key.ToString() + " is not in the last physical range in logical range: " + (*it)->toString());
        return false;
    }
    // `(*it)->endUserKey() < user_key && (*it)->endUserKey() != logical_range_end_key` is possible (tail insertion in a middle physical range)

    ParsedInternalKey parsed_internal_key;
    Status s = ParseInternalKey(internal_key, &parsed_internal_key, false);
    SequenceNumber key_seq_num = parsed_internal_key.sequence;

    // update in physical range
    PhysicalRangeUpdateResult updateResult = (*it)->update(internal_key, value);

    if (updateResult == PhysicalRangeUpdateResult::UNABLE_TO_INSERT) {
        logger.error("Failed to update entry (user key = " + parsed_internal_key.user_key.ToString() + ") in PhysicalRange: " + (*it)->toString());
        if (LogicalOrderedRangeCache::getPhysicalRangeType() == PhysicalRangeType::CONTINUOUS) {
            logger.error("Random insertion in continuous physical range is not supported yet");
        }
        return false;
    } else if (updateResult == PhysicalRangeUpdateResult::ERROR) {
        logger.error("Error updating entry (user key = " + parsed_internal_key.user_key.ToString() + ") in PhysicalRange: " + (*it)->toString());
        return false;
    } else if (updateResult == PhysicalRangeUpdateResult::OUT_OF_RANGE) {
        logger.error("Key " + parsed_internal_key.user_key.ToString() + " is out of range in PhysicalRange: " + (*it)->toString());
        return false;
    } else if (updateResult == PhysicalRangeUpdateResult::UPDATED) {
        // normally updated
        // Note: not re-calculate byte size of the PhysicalRange after update (even for VecPhysicalRange)
        // TODO(jr): neccessary to re-calculate the byte size of the PhysicalRange?
    } else if (updateResult == PhysicalRangeUpdateResult::INSERTED) {
        // update the outer logical range length
        range_it->setLength(range_it->length() + 1);
        
        // update lorc info
        this->total_range_length += 1;
        this->current_size += (internal_key.size() + value.size());
        while (this->current_size > this->capacity) {
            this->victim();
        }
    }

    assert(updateResult == PhysicalRangeUpdateResult::UPDATED || updateResult == PhysicalRangeUpdateResult::INSERTED);
    // Update the cache sequence number after updating an entry
    this->setRangeCacheSeqNum(std::max(this->getRangeCacheSeqNum(), key_seq_num));
    return true;
}

bool RBTreeLogicalOrderedRangeCache::Get(const Slice& internal_key, std::string* value, Status* s) const {
    lockRead();
    assert(s);
    
    // Extract user key from internal key
    ParsedInternalKey parsed_internal_key;
    Status status = ParseInternalKey(internal_key, &parsed_internal_key, false);
    if (!status.ok()) {
        *s = status;
        assert(false);
        unlockRead();
        return false;
    }
    Slice user_key = parsed_internal_key.user_key;
    SequenceNumber key_seq_num = parsed_internal_key.sequence;
    ValueType key_type = parsed_internal_key.type;

    if (key_seq_num < this->getRangeCacheSeqNum()) {
        // The key is older than the cache sequence number, not found
        logger.warn("Get: Key " + user_key.ToString() + " with sequence number " + std::to_string(key_seq_num) + " is older than cache sequence number " + std::to_string(this->getRangeCacheSeqNum()));
        *s = Status::OK();
        unlockRead();
        return false;
    }

    if (key_type != kValueTypeForSeek) {
        // The key type is not for seeking, return not found
        logger.error("Get: Key " + user_key.ToString() + " is not a valid seek key (type = " + std::to_string(static_cast<unsigned char>(key_type)) + ")");
        *s = Status::NotFound("Key is not a valid seek key");
        assert(false);
        unlockRead();
        return false;
    }
    
    // Find the physical range that may contain the key
    auto it = ordered_physical_ranges.upper_bound(user_key);
    if (it != ordered_physical_ranges.begin()) {
        it--;
    }
    
    // Check if we found a valid range and the key is within bounds
    if (it == ordered_physical_ranges.end() || (*it)->startUserKey() > user_key || (*it)->endUserKey() < user_key) {
        // Key not found in any physical range
        *s = Status::OK();
        unlockRead();
        return false;
    }
    
    // Use find function to locate the exact position of the key
    int index = (*it)->find(user_key);
    if (index < 0 || (size_t)index >= (*it)->length()) {
        assert(false);
        logger.error("Get: Key " + user_key.ToString() + " not found in PhysicalRange: " + (*it)->toString());
        *s = Status::NotFound("Key not found in PhysicalRange");
        unlockRead();
        return false;
    }
    
    // Check if the found key exactly matches our target key
    if ((*it)->userKeyAt(index) != user_key) {
        *s = Status::OK();
        unlockRead();
        return false;
    }
    
    // Key found, retrieve the value
    if (value) {
        *value = (*it)->valueAt(index).ToString();
    }
    *s = Status::OK();
    unlockRead();
    return true;
}

void RBTreeLogicalOrderedRangeCache::tryVictim() {    
    lockRead();
    // If no ranges exist, nothing to evict
    if (physical_range_length_map.empty() || ordered_physical_ranges.empty() || this->current_size <= this->capacity) {
        unlockRead();
        return;
    }

    // upgrade to unique lock for real victim
    unlockRead();
    lockWrite();
    while (this->current_size > this->capacity) {
        this->victim();
    }
    unlockWrite();
}

void RBTreeLogicalOrderedRangeCache::victim() {    
    // Evict the shortest PhysicalRange to minimize impact
    if (this->current_size <= this->capacity) {
        return;
    }
    if (physical_range_length_map.empty() || ordered_physical_ranges.empty()) {
        return;
    }
    
    // victimRangeStartKey is the start key of range whose len is the smallest
    // TODO(jr): victim better
    Slice victimRangeStartKey;
    Slice victimRangeEndKey;
    size_t min_len = 0;
    for (auto& range : ranges_view.getLogicalRanges()) {
        if (range.length() < min_len || min_len == 0) {
            min_len = range.length();
            victimRangeStartKey = range.startUserKey();
            victimRangeEndKey = range.endUserKey();
        }
    }

    // If multiple ranges exist, remove the victim
    // If only one PhysicalRange remains, do nothing
    if (ordered_physical_ranges.size() > 1 || this->capacity <= 0) {
        for (auto it = ordered_physical_ranges.begin(); it != ordered_physical_ranges.end();) {
            if ((*it)->startUserKey() >= victimRangeStartKey && 
                (*it)->endUserKey() <= victimRangeEndKey) {
                logger.debug("Victim: " + (*it)->toString());
                this->current_size -= (*it)->byteSize();
                this->total_range_length -= (*it)->length();
                
                // Remove from physical_range_length_map
                auto range = physical_range_length_map.equal_range((*it)->length());
                for (auto mapIt = range.first; mapIt != range.second; ++mapIt) {
                    if (mapIt->second == (*it)->startUserKey().ToString()) {
                        physical_range_length_map.erase(mapIt);
                        break;
                    }
                }
                
                // Erase from ordered_physical_ranges and move to next iterator
                it = ordered_physical_ranges.erase(it);
            } else {
                ++it;
            }
        }
        // Remove the logical range from ranges_view
        ranges_view.removeRange(victimRangeStartKey);
    } else {
        auto it = ordered_physical_ranges.find(victimRangeStartKey);
        assert(it != ordered_physical_ranges.end() && (*it)->startUserKey() == victimRangeStartKey);
        // Do nothing
        logger.info("Not victim the last range: " + (*it)->toString());
    }
}

void RBTreeLogicalOrderedRangeCache::pinRange(std::string startKey) {
    auto it = ordered_physical_ranges.find(startKey);
    if (it != ordered_physical_ranges.end() && (*it)->startUserKey() == startKey) {
        // update the timestamp
        (*it)->setTimestamp(this->cache_timestamp++);
    }
}

LogicalOrderedRangeCacheIterator* RBTreeLogicalOrderedRangeCache::newLogicalOrderedRangeCacheIterator(Arena* arena) const {
    if (arena == nullptr) {
        return new RBTreeLogicalOrderedRangeCacheIterator(this);
    }
    auto mem = arena->AllocateAligned(sizeof(RBTreeLogicalOrderedRangeCacheIterator));
    return new (mem) RBTreeLogicalOrderedRangeCacheIterator(this);
}

void RBTreeLogicalOrderedRangeCache::printAllPhysicalRanges() const {
    if (!logger.outputInLevel(LorcLogger::Level::DEBUG)) {
        return;
    }
    logger.debug("All physical ranges in RBTreeLogicalOrderedRangeCache:");
    for (auto it = ordered_physical_ranges.begin(); it != ordered_physical_ranges.end(); ++it) {
        logger.debug("PhysicalRange: " + (*it)->toString());
    }
    logger.debug("Total PhysicalRange size: " + std::to_string(this->current_size));
    logger.debug("Total PhysicalRange length: " + std::to_string(this->total_range_length));
    logger.debug("Total PhysicalRange num: " + std::to_string(this->ordered_physical_ranges.size()));
}

void RBTreeLogicalOrderedRangeCache::printAllLogicalRanges() const {
    if (!logger.outputInLevel(LorcLogger::Level::DEBUG)) {
        return;
    }
    logger.debug("All logical ranges in RBTreeLogicalOrderedRangeCache:");
    auto logical_ranges = this->ranges_view.getLogicalRanges();
    size_t total_len = 0;
    for (auto it = logical_ranges.begin(); it != logical_ranges.end(); ++it) {
        logger.debug("LogicalRange: " + it->toString());
        total_len += it->length();
    }
    logger.debug("Total LogicalRange length: " + std::to_string(total_len));
    logger.debug("Total LogicalRange num: " + std::to_string(logical_ranges.size()));
}

void RBTreeLogicalOrderedRangeCache::printAllRangesWithKeys() const {
    lockRead();
    if (!logger.outputInLevel(LorcLogger::Level::DEBUG)) {
        unlockRead();
        return;
    }

    logger.debug("\n----------------------------------------");
    this->printAllLogicalRanges();
    logger.debug("----------------------------------------");
    this->printAllPhysicalRanges();
    logger.debug("----------------------------------------\n");

    logger.debug("All keys in RBTreeLogicalOrderedRangeCache:");
    for (const auto& range : ordered_physical_ranges) {
        for (size_t i = 0; i < range->length(); ++i) {
            ParsedInternalKey parsed_key;
            Status s = ParseInternalKey(range->internalKeyAt(i), &parsed_key, false);
            logger.debug("User Key: " + parsed_key.user_key.ToString() + ", Seq = " + std::to_string(parsed_key.sequence) +
                         ", Type = " + std::to_string(static_cast<unsigned char>(parsed_key.type)));
        }
    }
    logger.debug("----------------------------------------");
    unlockRead();
}

void RBTreeLogicalOrderedRangeCache::lockRead() const {
    logical_ranges_mutex_.lock_shared();
}

void RBTreeLogicalOrderedRangeCache::lockWrite() {
    logical_ranges_mutex_.lock();
}

void RBTreeLogicalOrderedRangeCache::unlockRead() const {
    logical_ranges_mutex_.unlock_shared();
}

void RBTreeLogicalOrderedRangeCache::unlockWrite() {
    logical_ranges_mutex_.unlock();
}

std::vector<LogicalRange> RBTreeLogicalOrderedRangeCache::divideLogicalRange(const Slice& start_key, size_t len, const Slice& end_key) const {
    std::vector<LogicalRange> result;
    size_t total_length_in_range_cache = 0;
    
    // Get all logical ranges
    const auto& logical_ranges = ranges_view.getLogicalRanges();
    
    // Handle empty start_key (means start from the beginning)
    Slice current_key = start_key;
    
    // Calculate actual end position
    bool has_length_limit = (len > 0);
    bool has_end_key_limit = !end_key.empty();
    bool terminated = false;

    auto find_first_range = [&logical_ranges](const Slice& key) {
        auto it = std::lower_bound(logical_ranges.begin(), logical_ranges.end(), key,
            [](const LogicalRange& range, const Slice& key_) {
                return range.endUserKey() < key_;
            });
        return it;
    };

    auto range_it = find_first_range(current_key);

    int num = 0;
    // Iterate through all logical ranges to find overlapping parts with the query range
    for (; range_it != logical_ranges.end(); ++range_it) {
        num++;
        const auto& range = *range_it;
        Slice range_start = range.startUserKey();
        Slice range_end = range.endUserKey();

        // If current key hasn't reached the start of this range, there's a gap
        if (current_key < range_start) {
            // Calculate the end position of the gap
            Slice gap_end = range_start;
            
            // If there's an end key limit and the gap would exceed it, truncate the gap
            if (has_end_key_limit && gap_end > end_key) {
                gap_end = end_key;
                result.emplace_back(current_key.ToString(), gap_end.ToString(), 0, false, result.empty(), true);
                current_key = gap_end;
                break; // last right split
            }
            
            // Only add the gap if it's meaningful (i.e., current_key < gap_end)
            if (current_key < gap_end) {
                result.emplace_back(current_key.ToString(), gap_end.ToString(), 0, false, result.empty(), false);
                current_key = gap_end;
            }

            // If we've reached the end key, stop processing
            if (has_end_key_limit && current_key >= end_key) {
                terminated = true;
                break;
            }
        }
        
        // Check if current range overlaps with the query range
        if (current_key <= range_end && (end_key.empty() || range_start < end_key)) {
            // Calculate the start and end of the overlapping part
            Slice overlap_start = std::max(current_key, range_start);
            Slice overlap_end = range_end;
            
            // If there's an end key limit, truncate the overlapping part
            if (has_end_key_limit && overlap_end > end_key) {
                overlap_end = end_key;
            }
            
            // Only add if the overlapping part is meaningful
            if (overlap_start <= overlap_end) {
                size_t remaining_length = len - total_length_in_range_cache;
                size_t range_len = this->downwardEstimateLengthInRangeCache(overlap_start, overlap_end, remaining_length);
                result.emplace_back(overlap_start.ToString(), overlap_end.ToString(), range_len, true, true, true);
                total_length_in_range_cache += range_len;
                current_key = overlap_end;
            }
            
            // If we have a length limit and the length in range cache reached it, stop processing
            // We cannot estimate range length of non-hit ranges, so regard it as 0
            if (has_length_limit && total_length_in_range_cache >= len) {
                terminated = true;
                break;
            }
            // If we've reached the end key, stop processing
            if (has_end_key_limit && current_key >= end_key) {
                terminated = true;
                break;
            }
        }
    }
    
    // Handle the non-hit range that might exist
    if (!terminated) {
        // If there's no end key limit, or we haven't reached the end key, add the final gap
        Slice final_end = has_end_key_limit ? end_key : Slice();
        result.emplace_back(current_key.ToString(), final_end.ToString(), 0, false, result.empty(), true);
    }
    
    return result;
}

size_t RBTreeLogicalOrderedRangeCache::downwardEstimateLengthInRangeCache(const Slice& start_key, const Slice& end_key, size_t remaining_length) const {
    assert(!start_key.empty() && !end_key.empty() && start_key <= end_key);
    
    size_t total_length = 0;
    
    // Find the range that contains or comes after start_key
    auto start_it = ordered_physical_ranges.upper_bound(start_key);
    if (start_it != ordered_physical_ranges.begin()) {
        --start_it;
        // assert((*start_it)->endUserKey() >= start_key);
        if ((*start_it)->endUserKey() < start_key) {
            return 0; // not in any physical range
        }
    }
    
    // Find the range that contains or comes after end_key
    auto end_it = ordered_physical_ranges.upper_bound(end_key);
    if (end_it != ordered_physical_ranges.begin()) {
        --end_it;
        // assert((*end_it)->endUserKey() >= end_key);
    }

    assert(start_it != ordered_physical_ranges.end() && end_it != ordered_physical_ranges.end() && std::distance(start_it, end_it) >= 0);
    
    // Iterate from start_it to calculate total length
    for (auto it = start_it; it != ordered_physical_ranges.end(); ++it) {
        int start_index = 0;
        int end_index = (*it)->length() - 1;
        if ((*it)->startUserKey() < start_key && (*it)->endUserKey() >= start_key) {
            start_index = (*it)->find(start_key);
            assert(start_index >= 0);
        }

        if ((*it)->startUserKey() < end_key && (*it)->endUserKey() >= end_key) {
            end_index = (*it)->find(end_key);
            assert(end_index >= 0);
            if ((*it)->userKeyAt(end_index) != end_key) {
                end_index -= 1;
            }
        }

        assert(start_index <= end_index);
        // minus delete length to downward estimate
        int range_length = std::max(end_index - start_index + 1 - (int)(*it)->deleteLength(), 0);
        total_length += (end_index - start_index + 1);

        if (it == end_it) {
            break;
        }
        if (total_length >= remaining_length) {
            // If we have reached the remaining length, stop traversing
            break;
        }
    }

    return std::min(total_length, remaining_length);
}

}  // namespace ROCKSDB_NAMESPACE
