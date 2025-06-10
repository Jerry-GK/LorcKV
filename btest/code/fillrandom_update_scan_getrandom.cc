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
#define VALUE_LEN 1024

bool do_create = true;
bool do_scan = false;
bool do_get = false;
bool do_update = false;

bool enable_blob = false;
bool enable_blob_cache = true;
bool enable_lorc = false;
bool enable_timer = true;

const int start_key = 1000000; // Start key for range
const int end_key = 4999999;   // End key for range
const int total_len = end_key - start_key + 1;
const int max_range_query_len = total_len * 0.01; // 1% of total length
const int min_range_query_len = total_len * 0.01;
const int num_range_queries = 1000;

// block cache size will always be set to 64MB later if enable_blob is true
// size_t block_cache_size = 0; // 0MB
// size_t block_cache_size = 64 * 1024 * 1024; // 64MB
// size_t block_cache_size = 1024 * 1024 * 1024; // 1GB
size_t block_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

// size_t blob_cache_size = 0; // 0MB
// size_t blob_cache_size = (size_t)64 * 1024 * 1024; // 64MB
size_t blob_cache_size = (size_t)1024 * 1024 * 1024; // 1GB
// size_t blob_cache_size = (size_t)2048  * 1024 * 1024; // 2GB
// size_t blob_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

// size_t range_cache_size = 0; // 0MB
// size_t range_cache_size = (size_t)64 * 1024 * 1024; // 64MB
size_t range_cache_size = (size_t)1024 * 1024 * 1024; // 1GB
// size_t range_cache_size = (size_t)2048 * 1024 * 1024; // 2GB
// size_t range_cache_size = (size_t)4096 * 1024 * 1024; // 4GB

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

void execute_get(DB* db, Options options) {
    auto start = enable_timer ? high_resolution_clock::now() : high_resolution_clock::time_point();
    for (const auto& pair : keyValuePairs) {
        const std::string &key = pair.first;
        std::string value = pair.second;
        status = db->Get(ReadOptions(), key, &value);
        if (!status.ok()) {
            cerr << "Failed to get key: " << key << ", error: " << status.ToString() << endl;
        }
    }
    if (enable_timer) {
        auto end = high_resolution_clock::now();
        duration<double> time_span = duration_cast<duration<double>>(end - start);
        cout << "Get time: " << time_span.count() << " seconds" << endl;
    }
}

int main() {
    // Set database path based on blob flag
    string db_path = enable_blob ? "./db/test_db_blobdb" : "./db/test_db_rocksdb";

    // Configure database options
    Options options;
    options.create_if_missing = do_create;

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
    block_cache_size = enable_blob ? 64 * 1024 * 1024 : block_cache_size;
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
    
    // Create and populate database if required
    if (do_create) {
        // Generate key-value pairs and store in vector
        for (int i = start_key; i <= end_key; ++i) {
            std::string key = gen_key(i);
            // value = 10 * 
            std::string value = gen_value(i);
            keyValuePairs.emplace_back(key, value);
        }

        std::shuffle(keyValuePairs.begin(), keyValuePairs.end(), rng);

        execute_insert(db, options);
    }

    // Execute operations based on flags
    if (do_scan) {
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> range_len_dist(min_range_query_len, max_range_query_len);
        
        double total_query_time = 0.0;
        
        std::cout << "Executing " << num_range_queries << " random range queries..." << std::endl;
        std::cout << "Range length between " << min_range_query_len << " and " << max_range_query_len << std::endl;
        
        for (int i = 0; i < num_range_queries; i++) {
            // 随机生成范围长度
            int query_length = range_len_dist(gen);
            
            // 随机生成起始键，确保不会超出有效范围
            int max_start = end_key - query_length;
            std::uniform_int_distribution<> start_key_dist(start_key, max_start);
            int query_start_key = start_key_dist(gen);
            
            std::string start_key_str = gen_key(query_start_key);
            
            std::cout << "Query " << (i+1) << ": start key = " << query_start_key 
                      << ", length = " << query_length << std::endl;
                      
            // 开始计时
            auto query_start = high_resolution_clock::now();
            
            // 执行范围查询
            execute_scan(db, options, start_key_str, query_length);
            
            // 结束计时
            auto query_end = high_resolution_clock::now();
            duration<double> query_time = duration_cast<duration<double>>(query_end - query_start);
            
            total_query_time += query_time.count();
            std::cout << "Query " << (i+1) << " time: " << query_time.count() << 
                " seconds, avg time = " << total_query_time / (i + 1) << "seconds" <<  std::endl;
        }
        
        // 输出平均查询时间
        double avg_query_time = total_query_time / num_range_queries;
        std::cout << "\nAverage query time for " << num_range_queries 
                  << " random range queries: " << avg_query_time << " seconds\n" << std::endl;
    }

    if (do_get) {
        // shuffle again to get random keys for querying
        std::shuffle(keyValuePairs.begin(), keyValuePairs.end(), rng);
        execute_get(db, options);
    }

    // Clean up
    delete db;

    return 0;
}