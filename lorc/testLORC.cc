#include "LogicallyOrderedRangeCache.h"
#include "SegmentedRangeCache.h"

#include "Range.h"
#include <iostream>
#include <unordered_map>

const int start_key =  1000;
const int end_key = 9999;

int main() {
    LogicallyOrderedRangeCache* lorc = new SegmentedRangeCache();

    const int num_ranges = 100;
    const int num_queries = 100;
    const bool do_validation = true;

    std::unordered_map<std::string, std::string> standard_cache;

    // randomly generate num_ranges ranges
    for (int i = 0; i < num_ranges; i++) {
        int start = rand() % (end_key - start_key) + start_key;
        int end = rand() % (end_key - start) + start;
        std::vector<std::string> keys;
        std::vector<std::string> values;
        for (int j = start; j <= end; j++) {
            std::string key = std::to_string(j);
            std::string value = "value@" + std::to_string(j) + "@" + std::to_string(i);
            keys.push_back(key);
            values.push_back(value);
            standard_cache[key] = value;
        }
        lorc->putRange(Range(keys, values, end - start + 1)); 
    }

    // randomly generate num_queries queries
    bool validationSuccess = true;
    for (int i = 0; i < num_queries; i++) {
        int start = rand() % (end_key - start_key) + start_key;
        int end = rand() % (end_key - start) + start;
        Range result = lorc->getRange(std::to_string(start), std::to_string(end));
        if (result.isValid() && do_validation) {
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

    std::cout << "Hit rate: " << lorc->hitRate() << std::endl;
}
