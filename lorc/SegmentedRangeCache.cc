#include "SegmentedRangeCache.h"
#include <algorithm>

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

    // 查找所有与新Range重叠的旧Range
    auto it = ranges.begin();
    while (it != ranges.end()) {
        bool isOverlap = (newRange.endKey() >= it->startKey()) && 
                         (newRange.startKey() <= it->endKey());
        if (isOverlap) {
            // 更新合并后的范围边界
            mergedStart = std::min(mergedStart, it->startKey());
            mergedEnd = std::max(mergedEnd, it->endKey());
            overlappingRanges.push_back(*it);
            it = ranges.erase(it); // 移除旧Range
        } else {
            ++it;
        }
    }

    // 合并数据：新Range数据优先，旧Range数据保留非重叠部分
    std::vector<std::string> mergedKeys = newRange.getKeys();
    std::vector<std::string> mergedValues = newRange.getValues();

    // 添加旧Range中非重叠部分的数据
    for (const auto& oldRange : overlappingRanges) {
        // 处理旧Range左侧非重叠部分（startKey < mergedStart）
        if (oldRange.startKey() < mergedStart) {
            size_t splitIdx = 0;
            while (splitIdx < oldRange.getSize() && 
                   oldRange.keyAt(splitIdx) < mergedStart) {
                mergedKeys.push_back(oldRange.keyAt(splitIdx));
                mergedValues.push_back(oldRange.valueAt(splitIdx));
                splitIdx++;
            }
        }

        // 处理旧Range右侧非重叠部分（endKey > mergedEnd）
        if (oldRange.endKey() > mergedEnd) {
            size_t splitIdx = oldRange.getSize() - 1;
            while (splitIdx >= 0 && 
                   oldRange.keyAt(splitIdx) > mergedEnd) {
                mergedKeys.push_back(oldRange.keyAt(splitIdx));
                mergedValues.push_back(oldRange.valueAt(splitIdx));
                splitIdx--;
            }
        }
    }

    // 按键排序合并后的数据
    std::sort(mergedKeys.begin(), mergedKeys.end());
    auto last = std::unique(mergedKeys.begin(), mergedKeys.end());
    mergedKeys.erase(last, mergedKeys.end());

    // 创建合并后的新Range并插入缓存
    Range mergedRange(mergedKeys, mergedValues, mergedKeys.size());
    ranges.push_back(mergedRange);

    // 保持ranges按startKey有序
    std::sort(ranges.begin(), ranges.end(), 
        [](const Range& a, const Range& b) {
            return a.startKey() < b.startKey();
        });
}

Range SegmentedRangeCache::getRange(const std::string& start_key, const std::string& end_key) {
    // find the first range that overlaps with the new range
    for (int i = ranges.size() - 1; i >=0; i--) {
        if (ranges[i].startKey() >= start_key && ranges[i].endKey() <= end_key) {
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