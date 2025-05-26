// #include <iostream>
// #include <unordered_map>
// #include <iomanip>
// #include <fstream>
// #include <cassert>
// #include <chrono>

// #include "cache/LogicallyOrderedContRangeCache.h"
// #include "cache/RBTreeContRangeCache.h"
// #include "iterator/ContRangeCacheIterator.h"
// #include "range/ContRange.h"
// #include "logger/Logger.h"
// #include "storage/KVMap.h"
// #include "storage/KVMapIterator.h"

// const int num_keys_level = 100000;
// const int start_key = num_keys_level;
// const int end_key = 10 * num_keys_level - 1;
// const int key_size = std::to_string(end_key).length();
// const int value_size = 4096;
// const double cache_size_ratio = 0.25;

// const int min_range_len = 1;
// const int max_range_len = num_keys_level * 10 * 0.01;
// const int num_queries = 5000;

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

// // template<typename T>
// // void emplaceFromRangeCache(std::vector<std::string>& v, T&& s) {
// //     v.emplace_back(std::forward<T>(s));
// // }

// // template<typename T>
// // void emplaceFromKVMap(std::vector<std::string>& v, T&& s) {
// //     v.emplace_back(std::forward<T>(s));
// // }

// // seperated for profile
// void emplaceFromRangeCache(ContRange& r, const Slice& key, const Slice& value) {
//     // should be locked
//     r.emplace(key, value);
// }

// void emplaceFromKVMap(ContRange& r, const std::string& key, const std::string& value) {
//     // should be locked
//     Slice keySlice (key.data(), key.size());
//     Slice valueSlice (value.data(), value.size());
//     r.emplace(keySlice, valueSlice);
// }

// void executeRangeQuery(KVMap* kv_map, LogicallyOrderedContRangeCache* lorc, const std::string& start_key, int range_len) {    
//     Logger::info("Executing ContRange query: < StartKey = " + start_key + ", len = " + std::to_string(range_len) + ", EndKey = " \
//         + std::to_string((std::stoi(start_key) + range_len - 1)) + " >");
//     ContRange cont_range(true);
//     cont_range.reserve(range_len * key_size, range_len * value_size, range_len);

//     int len = 0;
//     ContRangeCacheIterator* rc_it = lorc->newContRangeCacheIterator();
//     rc_it->Seek(start_key);
//     KVMapIterator* kv_it = kv_map->newKVMapIterator(); // invalid at first
//     if (!(rc_it->Valid() && rc_it->key() == start_key)) {   // avoid unnecessary kv seek
//         kv_it->Seek(start_key);
//     }

//     bool full_hit = true;
//     bool partial_hit = false;
//     std::string last_read_rc_key = start_key;
//     long long get_range_total_time = 0;
//     while (len < range_len) {
//         if (rc_it->Valid() && (!kv_it->Valid() || rc_it->key() <= kv_it->key())) {
//             int hit_len = 0;
//             auto start_time = std::chrono::high_resolution_clock::now();
//             while (rc_it->Valid() && len < range_len) {
//                 bool is_last_range_key = !rc_it->HasNextInRange();
//                 if (is_last_range_key) {
//                     last_read_rc_key = rc_it->key().ToString();
//                 }
//                 emplaceFromRangeCache(cont_range, rc_it->key(), rc_it->value());
//                 partial_hit = true;
//                 len++;
//                 hit_len++;
//                 rc_it->Next();
//                 if (is_last_range_key) {
//                     break;
//                 }
//             }
//             auto end_time = std::chrono::high_resolution_clock::now();
//             auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
//             if (hit_len > 0) {
//                 get_range_total_time += duration;
//             }   
//             lorc->increaseHitSize(hit_len);
//             if (len < range_len) {
//                 kv_it->Seek(last_read_rc_key);
//                 if (kv_it->Valid() && kv_it->key() == last_read_rc_key) {
//                     kv_it->Next(); // skip the last key read in ContRange cache
//                 }
//             }
//         } else if (rc_it->Valid() && (kv_it->Valid() && rc_it->key() > kv_it->key())) {
//             emplaceFromKVMap(cont_range, kv_it->key(), kv_it->value());
//             len++;
//             full_hit = false;
//             kv_it->Next();
//         } else if (!rc_it->Valid()) {
//             if (kv_it->Valid()) {
//                 emplaceFromKVMap(cont_range, kv_it->key(), kv_it->value());
//                 len++;
//                 full_hit = false;
//                 kv_it->Next();
//             } else {
//                 break;
//             }
//         }
//     }
    
//     assert(cont_range.getSize() == range_len && range_len == len);
//     // if (do_validation) {
//     //     bool validation_success = kv_map->Validate(keys, values);
//     //     if (!validation_success) {
//     //         Logger::error("Validation failed for ContRange: < StartKey = " + start_key + ", len = " + std::to_string(range_len) + ", EndKey = " \
//     //             + std::to_string((std::stoi(start_key) + range_len - 1)) + " >");
//     //     }
//     // }   

//     if (!full_hit) {
//         lorc->putRange(std::move(cont_range)); 
//     } else {
//         lorc->increaseFullHitCount();
//     }
//     lorc->increaseFullQueryCount();
//     lorc->increaseQuerySize(range_len);

//     if (partial_hit) {
//         lorc->getCacheStatistic().increaseGetRangeNum();
//         lorc->getCacheStatistic().increaseGetRangeTotalTime(get_range_total_time);
//     }

//     delete rc_it;
//     delete kv_it;
// }

// int main() {
//     Logger::setEnabled(enable_logger);
//     Logger::setLevel(Logger::DEBUG); 
//     Logger::info("Start testing...");
//     int cache_size = (end_key - start_key + 1) * cache_size_ratio;
//     // LogicallyOrderedContRangeCache* lorc = new SegmentedContRangeCache(cache_size);
//     // LogicallyOrderedContRangeCache* lorc = new RowContRangeCache(cache_size);
//     LogicallyOrderedContRangeCache* lorc = new RBTreeContRangeCache(cache_size);
//     lorc->setEnableStatistic(true);

//     KVMap* kv_map = new KVMap();
    
//     std::vector<double> hitrates;
//     hitrates.reserve(num_queries);

//     // init standard_kv
//     for (int i = start_key; i <=end_key; i++) {
//         kv_map->Put(std::to_string(i), genValueStr(std::to_string(i)));
//     }

//     Logger::info("Standard KV map initialized! Start testing LORC...");
//     long long total_query_time = 0;
    
//     // randomly generate num_queries queries
//     for (int i = 1; i <= num_queries; i++) {
//         Logger::info("Query " + std::to_string(i) + " / " + std::to_string(num_queries) + " (" + std::to_string((double)i / num_queries * 100) + "%)");
        
//         int range_len = rand() % (max_range_len - min_range_len + 1) + min_range_len;
//         int start = rand() % (end_key - start_key - range_len) + start_key;
//         int end = start + range_len - 1;

//         auto start_time = std::chrono::high_resolution_clock::now();
//         executeRangeQuery(kv_map, lorc, std::to_string(start), range_len);
//         auto end_time = std::chrono::high_resolution_clock::now();
//         auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
//         total_query_time += duration;

//         Logger::info("Current full hit rate: " + std::to_string(lorc->fullHitRate() * 100) + "%");
//         Logger::info("Current hit size rate: " + std::to_string(lorc->hitSizeRate() * 100) + "%");
//         hitrates.emplace_back(lorc->fullHitRate());
//     }

//     // output cache statistic
//     if (lorc->enableStatistic()) {
//         Logger::info("Cache Statistic:");
//         Logger::info("Put ContRange Num: " + std::to_string(lorc->getCacheStatistic().getPutRangeNum()));
//         Logger::info("Get ContRange Num: " + std::to_string(lorc->getCacheStatistic().getGetRangeNum()));
//         Logger::info("Avg Put ContRange Time: " + std::to_string(lorc->getCacheStatistic().getAvgPutRangeTime()) + " us");
//         Logger::info("Avg Get ContRange Time: " + std::to_string(lorc->getCacheStatistic().getAvgGetRangeTime()) + " us");
//     }
    
//     Logger::info("Average Query Time: " + std::to_string(total_query_time / num_queries) + " us");

//     delete lorc;

//     return 0;
// }
