#include <iostream>
#include <vector>
#include <chrono>
#include <cstring>

constexpr size_t STRING_SIZE = 4096;   // Size of each string
constexpr size_t NUM_STRINGS = 1000000;  // Number of strings
constexpr size_t REPS = 1;            // Number of repetitions

// Allocate continuous memory: single large block
std::vector<char> create_continuous_memory() {
    return std::vector<char>(NUM_STRINGS * STRING_SIZE);
}

// Allocate non-continuous memory: separate allocation for each string
std::vector<char*> create_non_continuous_memory() {
    std::vector<char*> ptrs(NUM_STRINGS);
    for (size_t i = 0; i < NUM_STRINGS; ++i) {
        ptrs[i] = new char[STRING_SIZE];
    }
    return ptrs;
}

// Free non-continuous memory
void free_non_continuous_memory(std::vector<char*>& ptrs) {
    for (auto p : ptrs) delete[] p;
}

// Perform memcpy test
void run_test(const char* src_base, const std::vector<size_t>& src_offsets,
             char* dst_base, const std::vector<size_t>& dst_offsets) {
    for (size_t i = 0; i < NUM_STRINGS; ++i) {
        const char* src = src_base + src_offsets[i];
        char* dst = dst_base + dst_offsets[i];
        std::memcpy(dst, src, STRING_SIZE);
    }
}

int main() {
    // Initialize source and destination memory
    auto cont_src = create_continuous_memory();
    auto non_cont_src = create_non_continuous_memory();
    auto cont_dst = create_continuous_memory();
    auto non_cont_dst = create_non_continuous_memory();

    // Calculate offsets (sequential for continuous, random for non-continuous)
    std::vector<size_t> cont_offsets(NUM_STRINGS);
    std::vector<size_t> non_cont_offsets(NUM_STRINGS);
    for (size_t i = 0; i < NUM_STRINGS; ++i) {
        cont_offsets[i] = i * STRING_SIZE;
        non_cont_offsets[i] = rand() % (NUM_STRINGS * STRING_SIZE);
    }

    // Test Case 1: Continuous source -> Continuous destination
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t r = 0; r < REPS; ++r) {
        run_test(cont_src.data(), cont_offsets, cont_dst.data(), cont_offsets);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto time_b = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Test Case 4: Non-continuous source -> Non-continuous destination
    start = std::chrono::high_resolution_clock::now();
    for (size_t r = 0; r < REPS; ++r) {
        run_test(non_cont_src[0], non_cont_offsets, non_cont_dst[0], non_cont_offsets);
    }
    end = std::chrono::high_resolution_clock::now();
    auto time_a = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Output results
    std::cout << "Case B (Continuous->Continuous) Time: " << time_b << " ms\n";
    std::cout << "Case A (Non-continuous->Non-continuous) Time: " << time_a << " ms\n";
    std::cout << "Performance improvement factor: " << static_cast<double>(time_a) / time_b << "x\n";

    // Free memory
    free_non_continuous_memory(non_cont_src);
    free_non_continuous_memory(non_cont_dst);
    return 0;
}