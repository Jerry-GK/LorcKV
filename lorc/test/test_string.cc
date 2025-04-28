// #include <iostream>
// #include <vector>
// #include <string>
// #include <chrono>
// #include <cstdint>

// const size_t kStringSize = 4096;  // String size: 4KB
// const uint64_t kTotalSize = UINT64_C(1024) * 1024 * 1024 * 20;  // Use uint64_t for large sizes
// const uint64_t kNumStrings = kTotalSize / kStringSize;


// const std::string& getStr() {
//     static std::string str(kStringSize, 'A');  
//     return str;
// }
// int main() {
//     // Check for potential overflow
//     if (kNumStrings > SIZE_MAX) {
//         std::cerr << "Error: Total size too large for vector capacity" << std::endl;
//         return 1;
//     }

//     // Prepare test data
//     std::vector<std::string> vec;
//     vec.reserve(kNumStrings);  // Pre-allocate space

//     // Start timing
//     auto start = std::chrono::high_resolution_clock::now();

//     // Perform emplace_back operations
//     for (size_t i = 0; i < kNumStrings; ++i) {
//         vec.emplace_back(getStr());
//     }

//     std::cout << "Total strings: " << vec.size() << std::endl;
//     // End timing
//     auto end = std::chrono::high_resolution_clock::now();
//     auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

//     // Calculate throughput
//     double seconds = duration.count() / 1000000.0;
//     double throughput = (static_cast<double>(kTotalSize) / (1024.0 * 1024.0)) / seconds;  // MB/s

//     std::cout << "Total data size: " << (kTotalSize / (1024 * 1024)) << " MB" << std::endl;
//     std::cout << "Time elapsed: " << seconds << " seconds" << std::endl;
//     std::cout << "Throughput: " << throughput << " MB/s" << std::endl;

//     return 0;
// }
