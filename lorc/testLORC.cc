#include "LogicallyOrderedRangeCache.h"
#include "SegmentedRangeCache.h"

#include "Range.h"
#include <iostream>
#include <unordered_map>
#include <iomanip>

const int start_key =  1000;
const int end_key = 9999;

int main() {
    LogicallyOrderedRangeCache* lorc = new SegmentedRangeCache();

    const int min_range_len = 100;
    const int max_range_len = 300;
    const int num_queries = 1000;
    const double update_ratio = 0.2;
    const bool do_validation = true;

    std::unordered_map<std::string, std::string> standard_cache;

    // init standard_cache
    for (int i = start_key; i <=end_key; i++) {
        standard_cache[std::to_string(i)] = "value@" + std::to_string(i) + "@" + std::to_string(rand() % 100);
    }

    // randomly generate num_queries queries
    bool validationSuccess = true;
    for (int i = 0; i < num_queries; i++) {
        int range_len = rand() % (max_range_len - min_range_len + 1) + min_range_len;
        int start = rand() % (end_key - start_key - range_len) + start_key;
        int end = start + range_len - 1;

        Range result = lorc->getRange(std::to_string(start), std::to_string(end));
        if (!result.isValid()) {
            // cache miss
            std::vector<std::string> keys;
            std::vector<std::string> values;
            for (int j = start; j <= end; j++) {
                std::string key = std::to_string(j);
                // update_ratio for standard_cache has been updated
                if (rand() % 100 < update_ratio * 100) {
                    standard_cache[key] = "value@" + key + "@" + std::to_string(rand() % 100);
                }
                keys.push_back(key);
                values.push_back(standard_cache[key]);
            }
            lorc->putRange(Range(keys, values, end - start + 1)); 
        } else if (do_validation) {
            // cache hit
            std::vector<std::string> keys = result.getKeys();
            std::vector<std::string> values = result.getValues();

            for (int j = 0; j < keys.size(); j++) {
                std::string key = keys[j];
                std::string value = values[j];
                if (standard_cache.find(key) == standard_cache.end()) {
                    std::cout << "Key not found in standard cache: " << key << std::endl;
                    validationSuccess = false;
                    break;
                } else if (standard_cache[key] != value) {
                    std::cout << "Value mismatch for key " << key << ": expected " << standard_cache[key] << ", got " << value << std::endl;
                    validationSuccess = false;
                    break;
                }
            }
            if (!validationSuccess) {
                break;
            }
        }
    }

    if (validationSuccess) {
        std::cout << "Validation successful!" << std::endl;
    } else {
        std::cout << "Validation failed!" << std::endl;
    }

    // Print the hit rate precent, 3 digits after the decimal point
    std::cout << "Hit rate: " << std::fixed << std::setprecision(3) << lorc->hitRate() * 100 << "%" << std::endl;
}
