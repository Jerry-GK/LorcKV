// #include <iostream>
// #include <unordered_map>
// #include <iomanip>
// #include <fstream>
// #include "cache/LogicallyOrderedVecRangeCache.h"
// #include "cache/SegmentedVecRangeCache.h"
// #include "cache/RowVecRangeCache.h"
// #include "cache/RBTreeVecRangeCache.h"
// #include "range/VecRange.h"
// #include "logger/Logger.h"

// const int num_keys_level = 1000000;
// const int start_key = num_keys_level;
// const int end_key = 10 * num_keys_level - 1;
// const int value_size = 4096;
// const double cache_size_ratio = 0.25;

// const int min_range_len = 1;
// const int max_range_len = num_keys_level * 10 * 0.01;
// const int num_queries = 500;

// const bool enable_logger = true;
// const double update_ratio = 0.2;
// const bool do_validation = false;
// const bool do_update = false;

// std::string genValueStr(std::string key) {
//     std::string value_postfix = "value@" + key + "@" + std::to_string(rand() % 100);
//     int prefix_len = value_size - value_postfix.length();
//     if (prefix_len < 0) {
//         Logger::error("Value size is too small for key: " + key);
//         return "";
//     }
//     std::string value = std::string(prefix_len, 'a');
//     value += value_postfix;
//     return value;
// }

// int main() {
//     Logger::setEnabled(enable_logger);
//     Logger::setLevel(Logger::DEBUG); 
//     Logger::info("Start testing...");
//     int cache_size = (end_key - start_key + 1) * cache_size_ratio;
//     // LogicallyOrderedVecRangeCache* lorc = new SegmentedVecRangeCache(cache_size);
//     // LogicallyOrderedVecRangeCache* lorc = new RowVecRangeCache(cache_size);
//     LogicallyOrderedVecRangeCache* lorc = new RBTreeVecRangeCache(cache_size);
//     lorc->setEnableStatistic(true);

//     std::unordered_map<std::string, std::string> standard_kv;
    
//     std::vector<double> hitrates;
//     hitrates.reserve(num_queries);

//     // init standard_kv
//     for (int i = start_key; i <=end_key; i++) {
//         standard_kv[std::to_string(i)] = genValueStr(std::to_string(i));
//     }

//     Logger::info("Standard cache initialized! Start testing LORC...");
//     // randomly generate num_queries queries
//     bool validationSuccess = true;
//     for (int i = 1; i <= num_queries; i++) {
//         Logger::info("Query " + std::to_string(i) + " / " + std::to_string(num_queries) + " (" + std::to_string((double)i / num_queries * 100) + "%)");
        
//         int range_len = rand() % (max_range_len - min_range_len + 1) + min_range_len;
//         int start = rand() % (end_key - start_key - range_len) + start_key;
//         int end = start + range_len - 1;

//         const CacheResult& result = lorc->getRange(std::to_string(start), std::to_string(end));
//         if (result.isFullHit()) {
//             // full hit
//             if (do_validation) {
//                 // cache hit
//                 const Range& full_hit_range = result.getFullHitRange(); // auto moved
//                 std::vector<std::string>& keys = full_hit_range.getKeys();
//                 std::vector<std::string>& values = full_hit_range.getValues();
    
//                 for (int j = 0; j < keys.size(); j++) {
//                     std::string key = keys[j];
//                     std::string value = values[j];
//                     if (standard_kv.find(key) == standard_kv.end()) {
//                         Logger::error("Key not found in standard cache: " + key);
//                         validationSuccess = false;
//                         break;
//                     } else if (standard_kv[key] != value) {
//                         Logger::error("Value mismatch for key " + key + ": expected " + standard_kv[key] + ", got " + value);
//                         validationSuccess = false;
//                         break;
//                     }
//                 }
//                 if (!validationSuccess) {
//                     break;
//                 }
//             }
//         } else if (result.isPartialHit()) {
//             // partial hit
//             const std::vector<Range>& partial_hit_ranges = result.getPartialHitRanges();
//             for (size_t j = 0; j < partial_hit_ranges.size(); j++) {
//                 std::vector<std::string>& keys = partial_hit_ranges[j].getKeys();
//                 std::vector<std::string>& values = partial_hit_ranges[j].getValues();

//                 if (do_validation) {
//                     for (int k = 0; k < keys.size(); k++) {
//                         std::string key = keys[k];
//                         std::string value = values[k];
//                         if (standard_kv.find(key) == standard_kv.end()) {
//                             Logger::error("Key not found in standard cache: " + key);
//                             validationSuccess = false;
//                             break;
//                         } else if (standard_kv[key] != value) {
//                             Logger::error("Value mismatch for key " + key + ": expected " + standard_kv[key] + ", got " + value);
//                             validationSuccess = false;
//                             break;
//                         }
//                     }
//                     if (!validationSuccess) {
//                         break;
//                     }
//                 }                
//             }
//             // get the remaining data from standard_kv and putRange
//             std::vector<std::string> keys;
//             std::vector<std::string> values;
//             int cur = start, curPartialRangeIndex = 0;
//             while (cur <= end) {
//                 std::string curKey = std::to_string(cur);
//                 if (curKey >= partial_hit_ranges[curPartialRangeIndex].startKey() && curKey <= partial_hit_ranges[curPartialRangeIndex].endKey()) {
//                     // this key is in the partial hit range
//                     keys.emplace_back(curKey);
//                     values.emplace_back(std::move(partial_hit_ranges[curPartialRangeIndex].valueAt(cur - std::stoi(partial_hit_ranges[curPartialRangeIndex].startKey()))));
//                 } else {
//                     // this key is not in the partial hit range
//                     keys.emplace_back(curKey);
//                     values.emplace_back(standard_kv[curKey]);   // query from standard cache
//                     if (curPartialRangeIndex < partial_hit_ranges.size() - 1) {
//                         curPartialRangeIndex++;
//                     }
//                 }
//                 cur++;
//             }
//             lorc->putRange(Range(std::move(keys), std::move(values), end - start + 1));
//         } else {
//             // no hit
//             std::vector<std::string> keys;
//             std::vector<std::string> values;
//             for (int j = start; j <= end; j++) {
//                 std::string key = std::to_string(j);
//                 // update_ratio for standard_kv has been updated
//                 if (do_update && rand() % 100 < update_ratio * 100) {
//                     standard_kv[key] = genValueStr(key);
//                 }
//                 keys.emplace_back(key);
//                 values.emplace_back(standard_kv[key]);
//             }
//             lorc->putRange(Range(std::move(keys), std::move(values), end - start + 1)); 
//         }

//         Logger::info("Current full hit rate: " + std::to_string(lorc->fullHitRate() * 100) + "%");
//         Logger::info("Current hit size rate: " + std::to_string(lorc->hitSizeRate() * 100) + "%");
//         hitrates.emplace_back(lorc->fullHitRate());
//     }

//     if (validationSuccess) {
//         Logger::info("Validation successful!");
//     } else {
//         Logger::error("Validation failed!");
//     }

//     // Print the hit rate precent, 3 digits after the decimal point
//     // std::cout << "Hit rate: " << std::fixed << std::setprecision(3) << lorc->hitRate() * 100 << "%" << std::endl;   

//     // // caclate estimated coverage
//     // double selectivity = ((min_range_len + max_range_len) / (double)(end_key - start_key + 1)) / 2;
//     // double cover = 1.0;
//     // for (int i = 0; i < num_queries; i++) {
//     //     cover = cover * (1 - selectivity);
//     // }
//     // cover = 1 - cover;
//     // std::cout << "Estimated coverage: " << std::fixed << std::setprecision(3) << cover * 100 << "%" << std::endl;

//     // output hitrates to hitrates.csv
//     // std::ofstream hitrate_file("output/hitrates.csv");
//     // if (hitrate_file.is_open()) {
//     //     for (size_t i = 0; i < hitrates.size(); i++) {
//     //         hitrate_file << i + 1 << ", " << hitrates[i] << std::endl;
//     //     }
//     //     hitrate_file.close();
//     // } else {
//     //     Logger::error("Unable to open file");
//     // }

//     // output cache statistic
//     if (lorc->enableStatistic()) {
//         std::cout << "Cache Statistic:" << std::endl;
//         std::cout << "Put Range Num: " << lorc->getCacheStatistic().putRangeNum << std::endl;
//         std::cout << "Get Range Num: " << lorc->getCacheStatistic().getRangeNum << std::endl;
//         std::cout << "Avg Put Range Time: " << lorc->getCacheStatistic().getAvgPutRangeTime() << " us" << std::endl;
//         std::cout << "Avg Get Range Time: " << lorc->getCacheStatistic().getAvgGetRangeTime() << " us" << std::endl;
//     }

//     delete lorc;

//     return 0;
// }
