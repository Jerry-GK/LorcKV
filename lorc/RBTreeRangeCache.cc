#include "RBTreeRangeCache.h"
#include <algorithm>
#include <iostream>
#include <cassert>
#include <iomanip>
#include <algorithm>
#include <climits>

RBTreeRangeCache::RBTreeRangeCache(int max_size)
    : LogicallyOrderedRangeCache(max_size) {
}

RBTreeRangeCache::~RBTreeRangeCache() {
    by_start.clear();
    by_length.clear();
}

void RBTreeRangeCache::putRange(const Range& newRange) {
    // 合并数据：新Range数据优先，旧Range数据保留非重叠部分
    std::vector<Range> leftRanges; // at most one
    std::vector<Range> rightRanges; // at most one
    std::string newRangeStr = newRange.toString();

    // it points to the first range that may overlap with newRange
    auto it = by_start.upper_bound(newRange);
    if (it != by_start.begin()) {
        --it;
    }
    while (it != by_start.end() && it->startKey() <= newRange.endKey()) {
        if (it->endKey() < newRange.startKey()) {
            // no overlap
            ++it;
            continue;
        }

        // left
        if (it->startKey() < newRange.startKey() && it->endKey() >= newRange.startKey()) {
            int leftSplitIdx = it->find(newRange.startKey()) - 1;
            if (leftSplitIdx >= 0 && leftSplitIdx < it->getSize()) {
                leftRanges.push_back(std::move(it->subRangeMoved(0, leftSplitIdx)));
            }
        }

        // right
        if (it->endKey() > newRange.endKey() && it->startKey() <= newRange.endKey()) {
            int rightSplitIdx = it->find(newRange.endKey());
            if (rightSplitIdx >= 0 && rightSplitIdx < it->getSize() && it->keyAt(rightSplitIdx) == newRange.endKey()) {
                rightSplitIdx++;
            }
            if (rightSplitIdx >= 0 && rightSplitIdx < it->getSize()) {
                rightRanges.push_back(std::move(it->subRangeMoved(rightSplitIdx, it->getSize() - 1)));
            }
        }

        // remove the old range
        auto range = by_length.equal_range(it->getSize());
        for (auto itt = range.first; itt != range.second; ++itt) {
            if (itt->second == it->startKey()) {
                by_length.erase(itt);
                break;
            }
        }
        this->current_size -= it->getSize();
        it = by_start.erase(it);
    }
    
    // Range::concat left ranges + newRange + right ranges
    if (leftRanges.size() > 1 || rightRanges.size() > 1) {
        Logger::error("Left or right ranges size is greater than 1");
        return;
    }
    std::vector<Range> mergedRanges;
    mergedRanges.reserve(leftRanges.size() + 1 + rightRanges.size());  // 预分配空间
    // avoid deep copy here!
    for (auto& range : leftRanges) {
        mergedRanges.push_back(std::move(range));
    }
    mergedRanges.push_back(std::move(newRange));
    for (auto& range : rightRanges) {
        mergedRanges.push_back(std::move(range));
    }
    // Range mergedRange = Range::concatRanges(mergedRanges);
    Range mergedRange = Range::concatRangesMoved(mergedRanges);
    
    // add the new merged range
    by_length.emplace(mergedRange.getSize(), mergedRange.startKey());
    this->current_size += mergedRange.getSize();
    by_start.insert(std::move(mergedRange));

    // 触发淘汰
    while (this->current_size > this->max_size) {
        this->victim();
    }

    // print all ranges's startKey and endKey in order
    Logger::debug("----------------------------------------");
    Logger::debug("All ranges after putRange and victim: " + newRangeStr);
    for (auto it = by_start.begin(); it != by_start.end(); ++it) {
        Logger::debug("Range: " + it->toString());
    }
    Logger::debug("Total range size: " + std::to_string(this->current_size));
    Logger::debug("----------------------------------------");
}

CacheResult RBTreeRangeCache::getRange(const std::string& start_key, const std::string& end_key) {
    Logger::debug("Get Range: < " + start_key + " -> " + end_key + " >");
    CacheResult result(false, false, Range(false), {});
    std::vector<Range> partial_hit_ranges;

    // it points to the first range which overlaps with the query range
    auto it = by_start.upper_bound(Range({start_key, start_key}, {}, 1)); // the parameter is a virtual temp range
    if (it != by_start.begin()) {
        --it;
    }
    if (it != by_start.end()) {
        // check if the range is a full hit
        if (it->startKey() <= start_key && it->endKey() >= end_key) {
            // full hit
            full_hit_count++;
            this->pinRange(it->startKey());
            Range fullHitRange = it->subRange(start_key, end_key); // dont use subRangeMoved to preserve underlying data
            hit_size += fullHitRange.getSize();
            result = CacheResult(true, false, std::move(fullHitRange), {});
        } else {
            if (it->endKey() < start_key) {
                ++it; // at most check the next one
            }
            if (it!= by_start.end() && it->startKey() <= end_key && it->endKey() >= start_key) {
                // partial hit
                do {
                    // dont use subRangeMoved to preserve underlying data
                    partial_hit_ranges.push_back(std::move(it->subRange(
                        std::max(it->startKey(), start_key),
                        std::min(it->endKey(), end_key))));
                    this->pinRange(it->startKey());
                } while(++it != by_start.end() && it->startKey() <= end_key && it->endKey() >= start_key);
            } else {
                // no hit
                CacheResult result(false, false, Range(false), {});
            }
        }
    }

    full_query_count++;
    query_size += std::stoi(end_key) - std::stoi(start_key) + 1; // TODO(jr): delete this code

    if (!result.isFullHit() && !partial_hit_ranges.empty()) {
        // partial hit
        for (const auto& range : partial_hit_ranges) {
            hit_size += range.getSize();
        }
        // 使用std::move移动整个vector
        result = CacheResult(false, true, Range(false), std::move(partial_hit_ranges));
    } 

    return result;
}

void RBTreeRangeCache::victim() {
    // victim the shortest range
    if (this->current_size <= this->max_size) {
        return;
    }
    auto victimRangeStartKey = by_length.begin()->second;
    // find the range in by_start and remove it
    auto it = by_start.find(Range({victimRangeStartKey, victimRangeStartKey}, {}, 1));
    if (it == by_start.end() || it->startKey() != victimRangeStartKey) {
        Logger::error("Victim range not found in by_start");
        return;
    }
    if (by_start.size() > 1) {
        Logger::debug("Victim: " + it->toString());
        this->current_size -= by_length.begin()->first;
        by_start.erase(it);
        by_length.erase(by_length.begin());
    } else {
        // truncate the range
        Logger::debug("Truncate: " + it->toString());
        by_length.erase(by_length.find(it->getSize()));
        it->truncate(this->max_size);
        by_length.emplace(it->getSize(), it->startKey());
        this->current_size = it->getSize();
    }
}

void RBTreeRangeCache::pinRange(std::string startKey) {
    auto it = by_start.find(Range({startKey, startKey}, {}, 1));
    if (it != by_start.end() && it->startKey() == startKey) {
        // update the timestamp
        it->setTimestamp(this->cache_timestamp++);
    }
}

double RBTreeRangeCache::fullHitRate() const {
    if (full_query_count == 0) {
        return 0;
    }
    return (double)full_hit_count / full_query_count;
}

double RBTreeRangeCache::hitSizeRate() const {
    if (query_size == 0) {
        return 0;
    }
    return (double)hit_size / query_size;
}
