#include "SegmentedRangeCache.h"
#include <algorithm>
#include <iostream>
#include <cassert>
#include <iomanip>

SegmentedRangeCache::SegmentedRangeCache(int max_size)
    : LogicallyOrderedRangeCache(max_size) {
    entries = std::vector<SegmentedRangeCacheEntry>();
    cache_timestamp = 0;
}

SegmentedRangeCache::~SegmentedRangeCache() {
    entries.clear();
}

void SegmentedRangeCache::putRange(const Range& newRange) {
    std::vector<Range> overlappingRanges;
    std::string mergedStart = newRange.startKey();
    std::string mergedEnd = newRange.endKey();

    // ������������Range�ص��ľ�Range
    auto it = entries.begin();
    while (it != entries.end()) {
        bool isOverlap = (newRange.endKey() >= it->getRange().startKey()) && 
                         (newRange.startKey() <= it->getRange().endKey());
        if (isOverlap) {
            // ���ºϲ���ķ�Χ�߽�
            mergedStart = std::min(mergedStart, it->getRange().startKey());
            mergedEnd = std::max(mergedEnd, it->getRange().endKey());
            overlappingRanges.push_back(it->getRange());
            this->current_size -= it->getRange().getSize();
            it = entries.erase(it); // �Ƴ���Range
        } else {
            ++it;
        }
    }

    // �ϲ����ݣ���Range�������ȣ���Range���ݱ������ص�����
    std::vector<std::string> mergedKeys = newRange.getKeys();
    std::vector<std::string> mergedValues = newRange.getValues();

    // ��Ӿ�Range�з��ص����ֵ�����
    for (const auto& oldRange : overlappingRanges) {
        // �����Range�����ص����֣�startKey < newRange.startKey()
        if (oldRange.startKey() < newRange.startKey()) {
            std::vector<std::string> oldLeftKeys;
            std::vector<std::string> oldLeftValues;
            size_t splitIdx = 0;
            while (splitIdx < oldRange.getSize() && oldRange.keyAt(splitIdx) < newRange.startKey()) {
                oldLeftKeys.push_back(oldRange.keyAt(splitIdx));
                oldLeftValues.push_back(oldRange.valueAt(splitIdx));
                splitIdx++;
            }
            // push front
            mergedKeys.insert(mergedKeys.begin(), oldLeftKeys.begin(), oldLeftKeys.end());
            mergedValues.insert(mergedValues.begin(), oldLeftValues.begin(), oldLeftValues.end());
        }

        // �����Range�Ҳ���ص����֣�endKey > newRange.endKey())
        if (oldRange.endKey() > newRange.endKey()) {
            std::vector<std::string> oldRightKeys;
            std::vector<std::string> oldRightValues;
            size_t splitIdx = oldRange.getSize() - 1;
            while (splitIdx >= 0 && oldRange.keyAt(splitIdx) > newRange.endKey()) {
                oldRightKeys.push_back(oldRange.keyAt(splitIdx));
                oldRightValues.push_back(oldRange.valueAt(splitIdx));
                splitIdx--;
            }
            // reverse the order of oldRightKeys and oldRightValues
            std::reverse(oldRightKeys.begin(), oldRightKeys.end());
            std::reverse(oldRightValues.begin(), oldRightValues.end());
            // push back
            mergedKeys.insert(mergedKeys.end(), oldRightKeys.begin(), oldRightKeys.end());
            mergedValues.insert(mergedValues.end(), oldRightValues.begin(), oldRightValues.end());
        }
    }

    // ��������ϲ��������
    // std::sort(mergedKeys.begin(), mergedKeys.end());
    // auto last = std::unique(mergedKeys.begin(), mergedKeys.end());
    // mergedKeys.erase(last, mergedKeys.end());

    // �����ϲ������Range�����뻺��
    Range mergedRange(mergedKeys, mergedValues, mergedKeys.size());
    entries.push_back(SegmentedRangeCacheEntry(mergedRange, this->cache_timestamp++));

    // ����ranges��startKey����
    std::sort(entries.begin(), entries.end(), 
        [](const SegmentedRangeCacheEntry& a, const SegmentedRangeCacheEntry& b) {
            return a.getRange().startKey() < b.getRange().startKey();
        });

    this->current_size += mergedRange.getSize();

    // print all ranges's startKey and endKey in order
    std::cout << "----------------------------------------" << std::endl;
    std::cout << "All ranges after putRange: " << newRange.toString() << std::endl;
    // std::cout << "Merged range: " << mergedRange.toString() << std::endl;
    for (int i = 0; i < entries.size(); i++) {
        const Range& range = entries[i].getRange();
        std::cout << "Range[" << i+1 << "]: " + range.toString() << std::endl;
    }
    std::cout << "Total range size: " << this->current_size << std::endl;
    std::cout << "----------------------------------------" << std::endl;

    // victim
    while (this->current_size > this->max_size) {
        this->victim();
    }
}

Range SegmentedRangeCache::getRange(const std::string& start_key, const std::string& end_key) {
    // find the first range that overlaps with the new range
    for (size_t i = 0; i < this->entries.size(); i++) {
        if (entries[i].getRange().startKey() <= start_key && entries[i].getRange().endKey() >= end_key) {
            hit_count++;
            query_count++;
            this->pinRange(i);
            return entries[i].getRange();
        }
    }
    query_count++;
    return Range(false);
}

// void SegmentedRangeCache::victim() {
//     // victim a random range
//     if (this->current_size <= this->max_size) {
//         return;
//     }
//     int randomIndex = rand() % entries.size();
//     std::cout << "Victim: " << entries[randomIndex].getRange().toString() << std::endl;
//     this->current_size -= entries[randomIndex].getRange().getSize();
//     entries.erase(entries.begin() + randomIndex);
// }

// void SegmentedRangeCache::victim() {
//     // victim the LRU range
//     if (this->current_size <= this->max_size) {
//         return;
//     }
//     auto minRangeIt = std::min_element(entries.begin(), entries.end(),
//         [](const SegmentedRangeCacheEntry& a, const SegmentedRangeCacheEntry& b) {
//             return a.getTimestamp() < b.getTimestamp();
//         });
//     if (minRangeIt != entries.end()) {
//         std::cout << "Victim: " << minRangeIt->getRange().toString() << std::endl;
//         this->current_size -= minRangeIt->getRange().getSize();
//         entries.erase(minRangeIt);
//     } else {
//         std::cout << "No range to victim" << std::endl;
//     }
// }

void SegmentedRangeCache::victim() {
    // victim the shortest range
    if (this->current_size <= this->max_size) {
        return;
    }
    auto minRangeIt = std::min_element(entries.begin(), entries.end(),
        [](const SegmentedRangeCacheEntry& a, const SegmentedRangeCacheEntry& b) {
            return a.getRange().getSize() < b.getRange().getSize();
        });
    if (minRangeIt != entries.end()) {
        std::cout << "Victim: " << minRangeIt->getRange().toString() << std::endl;
        this->current_size -= minRangeIt->getRange().getSize();
        entries.erase(minRangeIt);
    } else {
        std::cout << "No range to victim" << std::endl;
    }
}

// void SegmentedRangeCache::victim() {
//     // victim the longest range
//     if (this->current_size <= this->max_size) {
//         return;
//     }
//     auto minRangeIt = std::min_element(entries.begin(), entries.end(),
//         [](const SegmentedRangeCacheEntry& a, const SegmentedRangeCacheEntry& b) {
//             return a.getRange().getSize() > b.getRange().getSize();
//         });
//     if (minRangeIt != entries.end()) {
//         std::cout << "Victim: " << minRangeIt->getRange().toString() << std::endl;
//         this->current_size -= minRangeIt->getRange().getSize();
//         entries.erase(minRangeIt);
//     } else {
//         std::cout << "No range to victim" << std::endl;
//     }
// }

void SegmentedRangeCache::pinRange(int index) {
    assert(index >= 0 && index < entries.size());
    entries[index].timestamp = this->cache_timestamp++;
}

double SegmentedRangeCache::hitRate() const {
    if (query_count == 0) {
        return 0;
    }
    return (double)hit_count / query_count;
}