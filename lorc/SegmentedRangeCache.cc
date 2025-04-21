#include "SegmentedRangeCache.h"
#include <algorithm>
#include <iostream>
#include <cassert>

SegmentedRangeCache::SegmentedRangeCache() {
    ranges = std::vector<Range>();
}

SegmentedRangeCache::~SegmentedRangeCache() {
    ranges.clear();
}

void SegmentedRangeCache::putRange(const Range& newRange) {
    std::vector<Range> overlappingRanges;
    std::string mergedStart = newRange.startKey();
    std::string mergedEnd = newRange.endKey();

    // ������������Range�ص��ľ�Range
    auto it = ranges.begin();
    while (it != ranges.end()) {
        bool isOverlap = (newRange.endKey() >= it->startKey()) && 
                         (newRange.startKey() <= it->endKey());
        if (isOverlap) {
            // ���ºϲ���ķ�Χ�߽�
            mergedStart = std::min(mergedStart, it->startKey());
            mergedEnd = std::max(mergedEnd, it->endKey());
            overlappingRanges.push_back(*it);
            it = ranges.erase(it); // �Ƴ���Range
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
    ranges.push_back(mergedRange);

    // ����ranges��startKey����
    std::sort(ranges.begin(), ranges.end(), 
        [](const Range& a, const Range& b) {
            return a.startKey() < b.startKey();
        });

    // print all ranges's startKey and endKey in order
    std::cout << "----------------------------------------" << std::endl;
    std::cout << "All ranges after putRange: " << newRange.toString() << std::endl;
    // std::cout << "Merged range: " << mergedRange.toString() << std::endl;
    size_t totalRangeSize = 0;
    for (int i = 0; i < ranges.size(); i++) {
        const Range& range = ranges[i];
        totalRangeSize += range.getSize();
        std::cout << "Range[" << i+1 << "]: " + range.toString() << std::endl;
    }
    std::cout << "Total range size: " << totalRangeSize << std::endl;
    std::cout << "----------------------------------------" << std::endl;
}

Range SegmentedRangeCache::getRange(const std::string& start_key, const std::string& end_key) {
    // find the first range that overlaps with the new range
    for (size_t i = 0; i < ranges.size(); i--) {
        if (ranges[i].startKey() <= start_key && ranges[i].endKey() >= end_key) {
            hit_count++;
            query_count++;
            return ranges[i];
        }
    }
    query_count++;
    return Range(false);
}

double SegmentedRangeCache::hitRate() const {
    if (query_count == 0) {
        return 0;
    }
    return (double)hit_count / query_count;
}