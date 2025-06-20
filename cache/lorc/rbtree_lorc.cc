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

RBTreeLogicallyOrderedRangeCache::RBTreeLogicallyOrderedRangeCache(size_t capacity_, LorcLogger::Level logger_level_, PhysicalRangeType physical_range_type_)
    : LogicallyOrderedRangeCache(capacity_, logger_level_, physical_range_type_), cache_timestamp(0) {
}

RBTreeLogicallyOrderedRangeCache::~RBTreeLogicallyOrderedRangeCache() {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_); // Lock for destruction
    orderedRanges.clear();
    lengthMap.clear();
    ranges_view.clear();
}

void RBTreeLogicallyOrderedRangeCache::putOverlappingRefRange(ReferringRange&& newRefRange) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_); // Acquire unique lock for writing
    std::chrono::high_resolution_clock::time_point start_time;
    if (this->enable_statistic) {
        start_time = std::chrono::high_resolution_clock::now();
    }

    std::vector<std::unique_ptr<PhysicalRange>> mergedRanges; 
    std::string newRangeStr = newRefRange.toString();

    // Find the first PhysicalRange that may overlap with newRefRange (keys in ref ranges are all user_key)
    auto it = orderedRanges.upper_bound(newRefRange.startKey());
    if (it != orderedRanges.begin()) {
        --it;
    }
    Slice lastStartKey = newRefRange.startKey();
    bool leftIncluded = true;
    while (it != orderedRanges.end() && (*it)->startUserKey() <= newRefRange.endKey()) {
        if ((*it)->endUserKey() < newRefRange.startKey()) {
            // no overlap
            ++it;
            continue;
        }

        // Handle left split: if existing PhysicalRange starts before newRefRange
        // If exists, it must be the first branch matched in the loop
        if ((*it)->startUserKey() < newRefRange.startKey() && (*it)->endUserKey() >= newRefRange.startKey()) {
            lastStartKey = (*it)->endUserKey();
            leftIncluded = false;
        } else if ((*it)->startUserKey() <= newRefRange.endKey()) {
            // overlapping without left split
            // masterialize the newRefRange of (lastStartKey, it->startUserKey())
            assert(lastStartKey <= (*it)->startUserKey());
            if (lastStartKey < (*it)->startUserKey()) {
                std::unique_ptr<PhysicalRange> nonOverlappingSubRange;
                if (LogicallyOrderedRangeCache::getPhysicalRangeType() == PhysicalRangeType::CONTINUOUS) {
                    nonOverlappingSubRange = ContinuousPhysicalRange::dumpSubRangeFromReferringRange(newRefRange, lastStartKey, newRefRange.endKey(), leftIncluded, true);
                } else if (LogicallyOrderedRangeCache::getPhysicalRangeType() == PhysicalRangeType::VEC) {
                    nonOverlappingSubRange = VecPhysicalRange::dumpSubRangeFromReferringRange(newRefRange, lastStartKey, newRefRange.endKey(), leftIncluded, true);
                } else {
                    logger.error("Unsupported PhysicalRangeType for RBTreeLogicallyOrderedRangeCache");
                    return;
                }
                if (!nonOverlappingSubRange || !nonOverlappingSubRange->isValid() || nonOverlappingSubRange->length() == 0) {
                    nonOverlappingSubRange.reset();
                }
                if (nonOverlappingSubRange && nonOverlappingSubRange->isValid() && nonOverlappingSubRange->length() > 0) {
                    // put into logical ranges view with right concation, with left concation if not leftIncluded
                    this->ranges_view.putLogicalRange(LogicalRange(nonOverlappingSubRange->startUserKey().ToString(), nonOverlappingSubRange->endUserKey().ToString(), nonOverlappingSubRange->length(), true, true, true), !leftIncluded, true);
                    mergedRanges.emplace_back(std::move(nonOverlappingSubRange));
                }
            }
            lastStartKey = (*it)->endUserKey();
            leftIncluded = false;
        }

        // Remove the old overlapping PhysicalRange from both containers
        auto physicalRangeStartKeys = lengthMap.equal_range((*it)->length());
        for (auto itt = physicalRangeStartKeys.first; itt != physicalRangeStartKeys.second; ++itt) {
            if (itt->second == (*it)->startUserKey()) {
                lengthMap.erase(itt);
                break;
            }
        }
        this->current_size -= (*it)->byteSize();
        this->total_range_length -= (*it)->length();
        
        // Move the old PhysicalRange in range cache to mergedRanges, and remove it (later merged as one large range into the range cache)
        // Using extract() without data copy
        auto current = it++;
        auto node = orderedRanges.extract(current);
        mergedRanges.emplace_back(std::move(node.value()));
    }

    // handle the last non-overlapping subrange if exists (case that has no right split)
    if (leftIncluded ? lastStartKey <= newRefRange.endKey() : lastStartKey < newRefRange.endKey()) {
        std::unique_ptr<PhysicalRange> nonOverlappingSubRange;
        if (LogicallyOrderedRangeCache::getPhysicalRangeType() == PhysicalRangeType::CONTINUOUS) {
            nonOverlappingSubRange = ContinuousPhysicalRange::dumpSubRangeFromReferringRange(newRefRange, lastStartKey, newRefRange.endKey(), leftIncluded, true);
        } else if (LogicallyOrderedRangeCache::getPhysicalRangeType() == PhysicalRangeType::VEC) {
            nonOverlappingSubRange = VecPhysicalRange::dumpSubRangeFromReferringRange(newRefRange, lastStartKey, newRefRange.endKey(), leftIncluded, true);
        } else {
            logger.error("Unsupported PhysicalRangeType for RBTreeLogicallyOrderedRangeCache");
            return;
        }
        if (!nonOverlappingSubRange || !nonOverlappingSubRange->isValid() || nonOverlappingSubRange->length() == 0) {
            nonOverlappingSubRange.reset();
        }
        if (nonOverlappingSubRange && nonOverlappingSubRange->isValid() && nonOverlappingSubRange->length() > 0) {
            // put into logical ranges view with left concation if not leftIncluded
            this->ranges_view.putLogicalRange(LogicalRange(nonOverlappingSubRange->startUserKey().ToString(), nonOverlappingSubRange->endUserKey().ToString(), nonOverlappingSubRange->length(), true, true, true), !leftIncluded, false);
            mergedRanges.emplace_back(std::move(nonOverlappingSubRange));
        }
    }
    
    // print mergedRanges
    for (auto& range : mergedRanges) {
        logger.debug("Merged PhysicalRange: " + range->toString());
    }

    // Add the new merged PhysicalRange to both containers
    for (auto& mergedRange : mergedRanges) {
        lengthMap.emplace(mergedRange->length(), mergedRange->startUserKey().ToString());
        this->current_size += mergedRange->byteSize();
        this->total_range_length += mergedRange->length();
        orderedRanges.emplace(std::move(mergedRange));
    }

    // Trigger eviction if cache size exceeds limit
    while (this->current_size > this->capacity) {
        this->victim();
    }

    // Debug output: print all ranges in order
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

    // deconstruction of the overlapping strings of old ranges with the deconstruction of leftRanges and rightRanges
}

void RBTreeLogicallyOrderedRangeCache::putGapPhysicalRange(ReferringRange&& newRefRange, bool leftConcat, bool rightConcat, bool emptyConcat, std::string emptyConcatLeftKey, std::string emptyConcatRightKey) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_); // Acquire unique lock for writing
    std::chrono::high_resolution_clock::time_point start_time;

    std::unique_ptr<PhysicalRange> newRange;
    if (LogicallyOrderedRangeCache::getPhysicalRangeType() == PhysicalRangeType::CONTINUOUS) {
        newRange = ContinuousPhysicalRange::buildFromReferringRange(newRefRange);
    } else if (LogicallyOrderedRangeCache::getPhysicalRangeType() == PhysicalRangeType::VEC) {
        newRange = VecPhysicalRange::buildFromReferringRange(newRefRange);
    } else {
        logger.error("Unsupported PhysicalRangeType for RBTreeLogicallyOrderedRangeCache");
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
        lengthMap.emplace(newRange->length(), newRange->startUserKey().ToString());
        this->current_size += newRange->byteSize();
        this->total_range_length += newRange->length();
        orderedRanges.emplace(std::move(newRange));
    } else {
        // empty actual range only for concat adjacent ranges
        assert(leftConcat && rightConcat && !emptyConcatLeftKey.empty() && !emptyConcatRightKey.empty());
        ranges_view.putLogicalRange(LogicalRange(emptyConcatLeftKey, emptyConcatRightKey, 0, true, true, true), true, true);
        // no need to change actual physical ranges info
    }


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
}

bool RBTreeLogicallyOrderedRangeCache::updateEntry(const Slice& internal_key, const Slice& value) {
    // TODO(jr): lock free here
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
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
        // The user key is not in any logical range, don NOT update
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
    auto it = orderedRanges.upper_bound(internal_key);
    if (it != orderedRanges.begin()) {
        it--;
    }
    if (it == orderedRanges.end() || (*it)->startUserKey() > user_key) { // (*it)->endUserKey() < user_key is possible in physical range
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
    SequenceNumber seq_num = parsed_internal_key.sequence;
    ValueType type = parsed_internal_key.type;

    // update in physical range
    PhysicalRangeUpdateResult updateResult = (*it)->update(internal_key, value);

    if (updateResult == PhysicalRangeUpdateResult::UNABLE_TO_INSERT) {
        logger.error("Failed to update entry (user key = " + parsed_internal_key.user_key.ToString() + ") in PhysicalRange: " + (*it)->toString());
        if (LogicallyOrderedRangeCache::getPhysicalRangeType() == PhysicalRangeType::CONTINUOUS) {
            logger.error("Random insertion in continuous physical range is not supported yet");
        }
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

    return updateResult == PhysicalRangeUpdateResult::UPDATED || updateResult == PhysicalRangeUpdateResult::INSERTED;
}

void RBTreeLogicallyOrderedRangeCache::tryVictim() {    
    std::unique_lock<std::shared_mutex> lock(cache_mutex_); // Acquire unique lock for writing
    // If no ranges exist, nothing to evict
    if (lengthMap.empty() || orderedRanges.empty()) {
        return;
    }

    while (this->current_size > this->capacity) {
        this->victim();
    }
}

void RBTreeLogicallyOrderedRangeCache::victim() {    
    // Evict the shortest PhysicalRange to minimize impact
    if (this->current_size <= this->capacity) {
        return;
    }
    if (lengthMap.empty() || orderedRanges.empty()) {
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
    if (orderedRanges.size() > 1 || this->capacity <= 0) {
        for (auto it = orderedRanges.begin(); it != orderedRanges.end();) {
            if ((*it)->startUserKey() >= victimRangeStartKey && 
                (*it)->endUserKey() <= victimRangeEndKey) {
                logger.debug("Victim: " + (*it)->toString());
                this->current_size -= (*it)->byteSize();
                this->total_range_length -= (*it)->length();
                
                // Remove from lengthMap
                auto range = lengthMap.equal_range((*it)->length());
                for (auto mapIt = range.first; mapIt != range.second; ++mapIt) {
                    if (mapIt->second == (*it)->startUserKey().ToString()) {
                        lengthMap.erase(mapIt);
                        break;
                    }
                }
                
                // Erase from orderedRanges and move to next iterator
                it = orderedRanges.erase(it);
            } else {
                ++it;
            }
        }
        // Remove the logical range from ranges_view
        ranges_view.removeRange(victimRangeStartKey);
    } else {
        auto it = orderedRanges.find(victimRangeStartKey);
        assert(it != orderedRanges.end() && (*it)->startUserKey() == victimRangeStartKey);
        // Do nothing
        logger.info("Not victim the last range: " + (*it)->toString());
    }
}

void RBTreeLogicallyOrderedRangeCache::pinRange(std::string startKey) {
    std::unique_lock<std::shared_mutex> lock(cache_mutex_);
    auto it = orderedRanges.find(startKey);
    if (it != orderedRanges.end() && (*it)->startUserKey() == startKey) {
        // update the timestamp
        (*it)->setTimestamp(this->cache_timestamp++);
    }
}

LogicallyOrderedRangeCacheIterator* RBTreeLogicallyOrderedRangeCache::newLogicallyOrderedRangeCacheIterator(Arena* arena) const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_); 
    if (arena == nullptr) {
        return new RBTreeLogicallyOrderedRangeCacheIterator(this);
    }
    auto mem = arena->AllocateAligned(sizeof(RBTreeLogicallyOrderedRangeCacheIterator));
    return new (mem) RBTreeLogicallyOrderedRangeCacheIterator(this);
}

void RBTreeLogicallyOrderedRangeCache::printAllPhysicalRanges() const {
    if (!logger.outputInLevel(LorcLogger::Level::DEBUG)) {
        return;
    }
    logger.debug("All physical ranges in RBTreeLogicallyOrderedRangeCache:");
    for (auto it = orderedRanges.begin(); it != orderedRanges.end(); ++it) {
        logger.debug("PhysicalRange: " + (*it)->toString());
    }
    logger.debug("Total PhysicalRange size: " + std::to_string(this->current_size));
    logger.debug("Total PhysicalRange length: " + std::to_string(this->total_range_length));
    logger.debug("Total PhysicalRange num: " + std::to_string(this->orderedRanges.size()));
}

void RBTreeLogicallyOrderedRangeCache::printAllLogicalRanges() const {
    if (!logger.outputInLevel(LorcLogger::Level::DEBUG)) {
        return;
    }
    logger.debug("All logical ranges in RBTreeLogicallyOrderedRangeCache:");
    auto logical_ranges = this->ranges_view.getLogicalRanges();
    size_t total_len = 0;
    for (auto it = logical_ranges.begin(); it != logical_ranges.end(); ++it) {
        logger.debug("LogicalRange: " + it->toString());
        total_len += it->length();
    }
    logger.debug("Total LogicalRange length: " + std::to_string(total_len));
    logger.debug("Total LogicalRange num: " + std::to_string(logical_ranges.size()));
}

void RBTreeLogicallyOrderedRangeCache::printAllRangesWithKeys() const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_); // Acquire shared lock for reading
    if (!logger.outputInLevel(LorcLogger::Level::DEBUG)) {
        return;
    }

    logger.debug("\n----------------------------------------");
    this->printAllLogicalRanges();
    logger.debug("----------------------------------------");
    this->printAllPhysicalRanges();
    logger.debug("----------------------------------------\n");

    logger.debug("All keys in RBTreeLogicallyOrderedRangeCache:");
    for (const auto& range : orderedRanges) {
        for (size_t i = 0; i < range->length(); ++i) {
            ParsedInternalKey parsed_key;
            Status s = ParseInternalKey(range->internalKeyAt(i), &parsed_key, false);
            logger.debug("User Key: " + parsed_key.user_key.ToString() + ", Seq = " + std::to_string(parsed_key.sequence) +
                         ", Type = " + std::to_string(static_cast<unsigned char>(parsed_key.type)));
        }
    }
    logger.debug("----------------------------------------");
}

std::vector<LogicalRange> RBTreeLogicallyOrderedRangeCache::divideLogicalRange(const Slice& start_key, size_t len, const Slice& end_key) const {
    std::shared_lock<std::shared_mutex> lock(cache_mutex_);
    
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

size_t RBTreeLogicallyOrderedRangeCache::downwardEstimateLengthInRangeCache(const Slice& start_key, const Slice& end_key, size_t remaining_length) const {
    assert(!start_key.empty() && !end_key.empty() && start_key <= end_key);
    
    size_t total_length = 0;
    
    // Find the range that contains or comes after start_key
    auto start_it = orderedRanges.upper_bound(start_key);
    if (start_it != orderedRanges.begin()) {
        --start_it;
        // assert((*start_it)->endUserKey() >= start_key);
        if ((*start_it)->endUserKey() < start_key) {
            return 0; // not in any physical range
        }
    }
    
    // Find the range that contains or comes after end_key
    auto end_it = orderedRanges.upper_bound(end_key);
    if (end_it != orderedRanges.begin()) {
        --end_it;
        // assert((*end_it)->endUserKey() >= end_key);
    }

    assert(start_it != orderedRanges.end() && end_it != orderedRanges.end() && std::distance(start_it, end_it) >= 0);
    
    // Iterate from start_it to calculate total length
    for (auto it = start_it; it != orderedRanges.end(); ++it) {
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
