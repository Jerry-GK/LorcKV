#pragma once

#include "Range.h"
#include <vector>
#include <set>
#include <map>
#include "LogicallyOrderedRangeCache.h"
#include "Logger.h"

class RBTreeRangeCache : public LogicallyOrderedRangeCache {
public:
RBTreeRangeCache(int max_size);
    ~RBTreeRangeCache() override;

    void putRange(Range&& range) override;
    CacheResult getRange(const std::string& start_key, const std::string& end_key) override;
    void victim() override;
    double fullHitRate() const override;
    double hitSizeRate() const override;

    void pinRange(std::string startKey);
    
private:
    std::set<Range> by_start; // ?start??
    std::multimap<int, std::string> by_length; // ?length????????
    int cache_timestamp;
};
