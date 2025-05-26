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

bool do_create = false;
bool do_scan = true;
bool do_get = false;
bool do_update = false;

bool enable_blob = true;
bool enable_blob_cache = false;
bool enable_lorc = true;
bool enable_timer = true;

const int start_key = 100000; // Start key for range
const int end_key = 999999;   // End key for range
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

// function gen key
std::string gen_key(int key) {
    std::string key_str = std::to_string(key);
    int prefix_len = KEY_LEN - key_str.length();
    assert(prefix_len >= 0);
    key_str.insert(0, prefix_len, '0');
    return key_str;
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

int main() {
    // Set database path based on blob flag
    string postfix = "_25%_update";
    string db_path = enable_blob ? "./db/test_db_blobdb" : "./db/test_db_rocksdb";
    // db_path += postfix;

    // Configure database options
    Options options;
    options.create_if_missing = do_create;

    // Configure blob settings if enabled
    options.enable_blob_files = enable_blob;
    std::shared_ptr<Cache> blob_cache = nullptr;
    if (enable_blob) {
        options.min_blob_size = 512;                
        options.blob_file_size = 64 * 1024 * 1024;  
        options.enable_blob_garbage_collection = false;
        options.blob_garbage_collection_age_cutoff = 0.25;
        if (enable_blob_cache) {
            blob_cache = rocksdb::NewLRUCache(blob_cache_size); 
            options.blob_cache = blob_cache;
        }
    }

    // Configure LORC if enabled
    std::shared_ptr<LogicallyOrderedSliceVecRangeCache> lorc = nullptr;
    if (enable_lorc) {
        lorc = rocksdb::NewRBTreeSliceVecRangeCache(range_cache_size, false);
        options.range_cache = lorc;
    }

    // Configure block cache
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

    // Clean up
    delete db;

    return 0;
}