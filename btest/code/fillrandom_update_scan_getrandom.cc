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
#define VALUE_LEN 4096

bool do_create = false;
bool do_scan = true;
bool do_get = false;
bool do_update = false;

bool enable_blob = true;
size_t cache_size = 8 * 1024 * 1024; // 8MB

const int start_key = 100000; // ��ʼ��
const int end_key = 999999;   // ������

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

void execute_insert(DB* db) {
    auto start = high_resolution_clock::now();
    // �����˳�����
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

void execute_scan(DB* db) {
    auto start = high_resolution_clock::now();
    Iterator* it = db->NewIterator(ReadOptions());
    vector<string> v;
    v.reserve(end_key - start_key + 1);
    // v.resize(end_key - start_key + 1);
    // for (int i = 0; i < v.size(); ++i) {
    //     v[i].resize(VALUE_LEN);
    // }
    
    // ���ToString��ʱ
    duration<double> toString_time(0);

    int i = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        
        // ��ʱ��ʼ
        auto toString_start = high_resolution_clock::now();
        std::string value = it->value().ToString();

        v.emplace_back(std::move(value));
        auto toString_end = high_resolution_clock::now();
        // memcpy(&v[i][0], value.c_str(), value.length());
        // auto toString_end = high_resolution_clock::now();

        // �ۼ�ʱ��
        toString_time += duration_cast<duration<double>>(toString_end - toString_start);
        ++i;
    }

    if (v.size() != end_key - start_key + 1) {
        std::cout << "v.size = " << v.size() << std::endl;
        assert(false);
    }

    delete it;
    auto end = high_resolution_clock::now();
    duration<double> time_span = duration_cast<duration<double>>(end - start);
    cout << "Scan time: " << time_span.count() << " seconds" << endl;
    cout << "ToString time: " << toString_time.count() << " seconds" << endl;
}

void execute_get(DB* db) {
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

int main() {
    // �������ݿ�·��
    string db_path = enable_blob ? "./db/test_db_blobdb" : "./db/test_db_rocksdb";

    // �������ݿ�ѡ��
    Options options;
    options.create_if_missing = do_create;

    options.enable_blob_files = enable_blob; 
    if (enable_blob) {
        options.min_blob_size = 512;                
        options.blob_file_size = 64 * 1024 * 1024;  
        options.enable_blob_garbage_collection = true;
        options.blob_garbage_collection_age_cutoff = 0.25;
    }

    // auto blob_cache = rocksdb::NewLRUCache(8 * 1024 * 1024); 
    // options.blob_cache = blob_cache;

    // 8MB
    auto cache = NewLRUCache(cache_size); // 8MB��LRU����
    BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    auto table_factory = NewBlockBasedTableFactory(table_options);
    options.table_factory.reset(table_factory);

    // �����ݿ�
    DB* db;
    status = DB::Open(options, db_path, &db);

    if (!status.ok()) {
        cerr << "Failed to open database: " << status.ToString() << endl;
        return 1;
    }

    // ʹ��������������������˳��
    std::random_device rd;
    std::mt19937 rng(rd());
    if (do_create) {
    // �������м�ֵ�Բ��洢��vector��
        for (int i = start_key; i <= end_key; ++i) {
            std::string key = gen_key(i);
            // value = 10 * 
            std::string value = gen_value(i);
            keyValuePairs.emplace_back(key, value);
        }

        std::shuffle(keyValuePairs.begin(), keyValuePairs.end(), rng);

        execute_insert(db);
    }

    if (do_scan) {
        execute_scan(db);
    }

    if (do_get) {
        // shuffle again to get random keys for querying
        std::shuffle(keyValuePairs.begin(), keyValuePairs.end(), rng);
        execute_get(db);
    }

    // �ر����ݿ�
    delete db;

    return 0;
}