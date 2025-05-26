#include <iostream>
#include <string>
#include <chrono>
#include <random>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table.h"
#include "rocksdb/vec_lorc.h"
#include "rocksdb/vec_range.h"
#include "rocksdb/ref_vec_range.h"

using namespace std;
using namespace rocksdb;
using namespace chrono;

#define KEY_LEN 24
#define VALUE_LEN 4096

bool do_scan = true;
bool do_update = true;

bool enable_blob = false;
bool enable_blob_cache = true;
bool enable_lorc = true;
bool enable_timer = true;

const int start_key = 100000; // Start key for range
const int end_key = 999999;   // End key for range
const int total_len = end_key - start_key + 1;
const int max_range_query_len = total_len * 0.01; // 1% of total length
const int min_range_query_len = total_len * 0.01;
const int num_range_queries = 1000;

double p_update = 0.2;  // 20% update before each scan
int num_update = 1000;   // num of entries to update of each update

// block cache size will always be set to 64MB later if enable_blob is true
// size_t block_cache_size = 0; // 0MB
// size_t block_cache_size = 64 * 1024 * 1024; // 64MB
// size_t block_cache_size = 1024 * 1024 * 1024; // 1GB
size_t block_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

// size_t blob_cache_size = 0; // 0MB
// size_t blob_cache_size = (size_t)64 * 1024 * 1024; // 64MB
// size_t blob_cache_size = (size_t)1024 * 1024 * 1024; // 1GB
// size_t blob_cache_size = (size_t)2048  * 1024 * 1024; // 2GB
size_t blob_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

// size_t range_cache_size = 0; // 0MB
// size_t range_cache_size = (size_t)64 * 1024 * 1024; // 64MB
// size_t range_cache_size = (size_t)1024 * 1024 * 1024; // 1GB
// size_t range_cache_size = (size_t)2048 * 1024 * 1024; // 2GB
size_t range_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

std::vector<std::pair<std::string, std::string>> keyValuePairs;
Status status;

std::string generateRandomString(int len) {
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<> dist(0, 9);
    std::string result(len, '0');

    for (int i = 0; i < len; ++i) {
        result[i] = '0' + dist(gen);
    }
    
    // int rand = std::rand();
    // std::string result = to_string(rand);
    // result += std::string(len - result.length(), '0');

    return result;
}

// function gen key
std::string gen_key(int key) {
    std::string key_str = std::to_string(key);
    int prefix_len = KEY_LEN - key_str.length();
    assert(prefix_len >= 0);
    key_str.insert(0, prefix_len, '0');
    return key_str;
}

// function gen value
std::string gen_value(int key) {
    std::string key_str = gen_key(key);
    int prefix_len = VALUE_LEN - key_str.length();
    assert(prefix_len >= 0);
    // gen a rand string of length prefix_len
    std::string value_str = generateRandomString(prefix_len) + key_str;
    return value_str;
}

void execute_insert(DB* db, Options options) {
    auto start = enable_timer ? high_resolution_clock::now() : high_resolution_clock::time_point();
    // Insert key-value pairs into database
    for (const auto& pair : keyValuePairs) {
        const std::string& key = pair.first;
        const std::string& value = pair.second;

        rocksdb::WriteOptions writeOptions;
        writeOptions.disableWAL = true;
        rocksdb::Status status = db->Put(writeOptions, key, value);

        if (!status.ok()) {
            std::cerr << "Failed to insert key: " << key << ", error: " << status.ToString() << std::endl;
            break;
        }
    }
    
    if (enable_timer) {
        auto end = high_resolution_clock::now();
        duration<double> time_span = duration_cast<duration<double>>(end - start);
        cout << "Insert time: " << time_span.count() << " seconds" << endl;
    }
}

void execute_scan(DB* db, Options options, std::string scan_start_key = "" , int len = -1) {
    auto start = enable_timer ? high_resolution_clock::now() : high_resolution_clock::time_point();
    if (len == -1) {
        len = end_key - start_key + 1;
    }

    ReadOptions read_options;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    
    Status s = db->Scan(read_options, Slice(scan_start_key), len, &keys, &values);
    assert(s.ok());
    assert(keys.size() == values.size());
    assert(keys.size() == len);

    if (enable_timer) {
        auto end = high_resolution_clock::now();
        duration<double> time_span = duration_cast<duration<double>>(end - start);
        cout << "\tScan total time: " << time_span.count() << " seconds" << endl;
    }
}

void execute_random_update(DB* db, Options options, int num_updates) {
    auto start = enable_timer ? high_resolution_clock::now() : high_resolution_clock::time_point();
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> key_dist(start_key, end_key);
    
    for (int i = 0; i < num_updates; i++) {
        int random_key = key_dist(gen);
        std::string key = gen_key(random_key);
        std::string value = gen_value(random_key);
        
        rocksdb::WriteOptions writeOptions;
        writeOptions.disableWAL = true;
        rocksdb::Status status = db->Put(writeOptions, key, value);
        
        if (!status.ok()) {
            std::cerr << "Failed to update key: " << key << ", error: " << status.ToString() << std::endl;
            break;
        }
    }
    
    if (enable_timer) {
        auto end = high_resolution_clock::now();
        duration<double> time_span = duration_cast<duration<double>>(end - start);
        cout << "\tRandom update time (" << num_updates << " records): " << time_span.count() << " seconds" << endl;
    }
}

int main() {
    // Set database path based on blob flag
    string db_path = enable_blob ? "./db/test_db_blobdb" : "./db/test_db_rocksdb";

    // Configure database options
    Options options;
    options.create_if_missing = false;
    // Direct IO
    options.use_direct_reads = true;      
    options.use_direct_io_for_flush_and_compaction = true;

    // Configure blob settings if enabled
    options.enable_blob_files = enable_blob;
    std::shared_ptr<Cache> blob_cache = nullptr;
    if (enable_blob) {
        options.min_blob_size = 512;                
        options.blob_file_size = 64 * 1024 * 1024;  
        options.enable_blob_garbage_collection = true;
        options.blob_garbage_collection_age_cutoff = 0.25;
        if (enable_blob_cache) {
            blob_cache = rocksdb::NewLRUCache(blob_cache_size); 
            options.blob_cache = blob_cache;
        }
    }

    // Configure LORC if enabled
    std::shared_ptr<LogicallyOrderedSliceVecRangeCache> lorc = nullptr;
    if (enable_lorc) {
        lorc = rocksdb::NewRBTreeSliceVecRangeCache(range_cache_size);
        options.range_cache = lorc;
    }

    // Configure block cache
    // block_cache_size = enable_blob ? 64 * 1024 * 1024 : block_cache_size;
    auto block_cache = NewLRUCache(block_cache_size);
    BlockBasedTableOptions table_options;
    table_options.block_cache = block_cache;
    auto table_factory = NewBlockBasedTableFactory(table_options);
    options.table_factory.reset(table_factory);

    // Open database
    DB* db;
    status = DB::Open(options, db_path, &db);

    if (!status.ok()) {
        cerr << "Failed to open database: " << status.ToString() << endl;
        return 1;
    }

    // Use random generator for key order
    std::random_device rd;
    std::mt19937 rng(rd());
    
    // Execute operations based on flags
    if (do_scan) {
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> range_len_dist(min_range_query_len, max_range_query_len);
        std::uniform_real_distribution<> update_prob(0.0, 1.0);
        
        double total_query_time = 0.0;
        double total_update_time = 0.0;  // 记录总更新时间
        int total_updates = 0;           // 记录总更新次数
        int total_updated_records = 0;   // 记录总更新数据条数
        
        std::cout << "Executing " << num_range_queries << " random range queries..." << std::endl;
        std::cout << "Range length between " << min_range_query_len << " and " << max_range_query_len << std::endl;
        
        int warmup_queries = static_cast<int>(num_range_queries * 0.2);

        for (int i = 0; i < num_range_queries; i++) {
            // 随机生成范围长度
            int query_length = range_len_dist(gen);
            
            // 随机生成起始键，确保不会超出有效范围
            int max_start = end_key - query_length;
            std::uniform_int_distribution<> start_key_dist(start_key, max_start);
            int query_start_key = start_key_dist(gen);
            
            std::string start_key_str = gen_key(query_start_key);
            
            // 计算预热阶段查询数量
            bool is_warmup = (i < warmup_queries);
            
            std::cout << "Query " << (i+1) << ": start key = " << query_start_key 
                      << ", length = " << query_length;
            if (is_warmup) {
                std::cout << " (WARMUP)";
            }
            std::cout << std::endl;
            
            // 在执行扫描前，根据概率决定是否进行更新
            if (do_update && update_prob(gen) < p_update) {
                std::cout << "Performing random update before query " << (i+1) << std::endl;
                
                auto update_start = high_resolution_clock::now();
                execute_random_update(db, options, num_update);
                auto update_end = high_resolution_clock::now();
                
                duration<double> update_time = duration_cast<duration<double>>(update_end - update_start);
                total_update_time += update_time.count();
                total_updates++;
                total_updated_records += num_update;
            }
            
            // 开始计时 - 只对非预热阶段的查询计时
            auto query_start = high_resolution_clock::now();
            
            // 执行范围查询
            execute_scan(db, options, start_key_str, query_length);
            
            // 结束计时
            auto query_end = high_resolution_clock::now();
            duration<double> query_time = duration_cast<duration<double>>(query_end - query_start);
            
            // 只统计非预热阶段的查询时间
            if (!is_warmup) {
                total_query_time += query_time.count();
            }
            
            if (!is_warmup) {
                std::cout << "\tAvg scan time = " << total_query_time / (i + 1 - warmup_queries) << " seconds";
            } else {
                std::cout << "\tWarm up, not counted in statistics";
            }
            std::cout << std::endl;
        }
                  
        // 输出更新统计信息
        if (total_updates > 0) {
            double avg_update_time = total_update_time / total_updates;
            std::cout << "Total updates: " << total_updates << " times, " 
                      << total_updated_records << " records" << std::endl;
            std::cout << "Average update time: " << avg_update_time 
                      << " seconds, Total update time: " << total_update_time << " seconds" << std::endl;
        }

        // 输出平均查询时间和更新统计
        double avg_query_time = total_query_time / (num_range_queries - warmup_queries);
        std::cout << "\nAverage query time for " << (num_range_queries - warmup_queries) 
                  << " random range queries (after " << warmup_queries << " warmup queries): " 
                  << avg_query_time << " seconds" << std::endl;

        std::cout << std::endl;
    }

    // Clean up
    delete db;

    return 0;
}