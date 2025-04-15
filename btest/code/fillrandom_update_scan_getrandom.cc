#include <iostream>
#include <string>
#include <chrono>
#include <random>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table.h"

using namespace std;
using namespace rocksdb;
using namespace chrono;

#define KEY_LEN 24
#define VALUE_LEN 2000

const int start_key = 100000; // 起始键
const int end_key = 999999;   // 结束键

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


int main() {
    // 设置数据库路径
    string db_path = "./db/test_db";

    // 创建数据库选项
    Options options;
    options.create_if_missing = true;

    // 8MB
    auto cache = NewLRUCache(8 * 1024 * 1024); // 8MB的LRU缓存
    BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    auto table_factory = NewBlockBasedTableFactory(table_options);
    options.table_factory.reset(table_factory);

    // 打开数据库
    DB* db;
    Status status = DB::Open(options, db_path, &db);

    if (!status.ok()) {
        cerr << "Failed to open database: " << status.ToString() << endl;
        return 1;
    }


    std::vector<std::pair<std::string, std::string>> keyValuePairs;

    // 生成所有键值对并存储在vector中
    for (int i = start_key; i <= end_key; ++i) {
        std::string key = gen_key(i);
        // value = 10 * 
        std::string value = gen_value(i);
        keyValuePairs.emplace_back(key, value);
    }

    // 使用随机数生成器生成随机顺序
    std::random_device rd;
    std::mt19937 rng(rd());
    std::shuffle(keyValuePairs.begin(), keyValuePairs.end(), rng);

    // 插入数据
    {
        auto start = high_resolution_clock::now();
        // 按随机顺序插入
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
        
        auto end = high_resolution_clock::now();
        duration<double> time_span = duration_cast<duration<double>>(end - start);
        cout << "Insert time: " << time_span.count() << " seconds" << endl;
    }

    // 扫描数据
    {
        auto start = high_resolution_clock::now();
        Iterator* it = db->NewIterator(ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string key = it->key().ToString();
            std::string value = it->value().ToString();
            // 无需处理结果
            // std::cout << "Key: " << key << ", Value: " << value << std::endl;
        }
        delete it;
        auto end = high_resolution_clock::now();
        duration<double> time_span = duration_cast<duration<double>>(end - start);
        cout << "Scan time: " << time_span.count() << " seconds" << endl;
    }

    // // 查询数据
    // {
    //     auto start = high_resolution_clock::now();
    //     for (int i = start_key; i <= end_key; i++) {
    //         string key = to_string(start_key + rand() % end_key);
    //         string value;
    //         status = db->Get(ReadOptions(), key, &value);
    //         if (!status.ok()) {
    //             cerr << "Failed to get key: " << key << ", error: " << status.ToString() << endl;
    //         }
    //     }
    //     auto end = high_resolution_clock::now();
    //     duration<double> time_span = duration_cast<duration<double>>(end - start);
    //     cout << "Get time: " << time_span.count() << " seconds" << endl;
    // }

    // 查询数据
    // shuffle again to get random keys for querying
    std::shuffle(keyValuePairs.begin(), keyValuePairs.end(), rng);
    {
        auto start = high_resolution_clock::now();
        for (const auto& pair : keyValuePairs) {
            const std::string &key = pair.first;
            std::string value = pair.second;
            status = db->Get(ReadOptions(), key, &value);
            if (!status.ok()) {
                cerr << "Failed to get key: " << key << ", error: " << status.ToString() << endl;
            }
        }
        auto end = high_resolution_clock::now();
        duration<double> time_span = duration_cast<duration<double>>(end - start);
        cout << "Get time: " << time_span.count() << " seconds" << endl;
    }

    // 关闭数据库
    delete db;

    return 0;
}