#pragma once

#include <vector>
#include "LogicallyOrderedRangeCache.h"

class SegmentedRangeCache : public LogicallyOrderedRangeCache {
public:
    SegmentedRangeCache();
    ~SegmentedRangeCache() override;

    void putRange(const Range& range) override;
    Range getRange(const std::string& start_key, const std::string& end_key) override;
    double hitRate() const override;

private:
    std::vector<Range> ranges;
};