#pragma once
#include <string>
#include <atomic>
#include <thread>
#include <memory>
#include <chrono>
#include <vector>
#include <mutex>
#include "db.hpp"
#include "direct_reader.hpp"

enum class ApproximationStatus {
    STABLE,
    DRIFTING,
    INSUFFICIENT_DATA,
    ERROR
};

struct ValidationResult {
    double value;
    ApproximationStatus status;
    double confidence_level;
    double error_margin;
    int samples_used;
    std::chrono::milliseconds computation_time;
};

class AdaptiveSampler {
public:
    AdaptiveSampler(const std::string& db_path, double error_threshold = 0.05, int num_threads = 4);
    ~AdaptiveSampler();
    
    // Main interface for adaptive sampling
    ValidationResult execute_adaptive_query(const std::string& query, 
                                           int initial_sample_percent = 10,
                                           double confidence_target = 0.95);
    
    // Block sampling interface 
    ValidationResult execute_block_sampling(const std::string& query,
                                           int block_size_percent = 10,
                                           double confidence_target = 0.95);
    
    // Fast-only block sampling (no validation overhead)
    ValidationResult execute_fast_block_sampling(const std::string& query,
                                                int block_size_percent = 10);
    
    // Multi-threaded fast block sampling for large datasets
    ValidationResult execute_parallel_fast_sampling(const std::string& query,
                                                   int block_size_percent = 10);
    
    // NEW: Direct file-level B-tree sampling (bypasses SQLite engine)
    ValidationResult execute_direct_file_sampling(const std::string& query,
                                                 int block_size_percent = 10);
    
    // NEW: Parallel direct file-level sampling
    ValidationResult execute_parallel_direct_sampling(const std::string& query,
                                                     int block_size_percent = 10,
                                                     int num_threads = 4);
    
    // Stop ongoing sampling
    void stop();
    
private:
    // Multi-threaded fast pointer implementation
    double multi_fast_pointer_sample(const std::string& query, int sample_percent);
    double fast_pointer_sample(const std::string& query, int sample_percent, int thread_offset = 0);
    
    // Block sampling implementation
    double block_sample(const std::string& query, int block_size_percent, int thread_offset = 0);
    double multi_block_sample(const std::string& query, int block_size_percent);
    
    // Fast block sampling (single-threaded, no validation)
    double fast_block_sample_only(const std::string& query, int block_size_percent);
    
    // Parallel fast block sampling
    double parallel_fast_block_sample(const std::string& query, int block_size_percent, int thread_id, int total_threads);
    std::vector<double> multi_parallel_fast_sample(const std::string& query, int block_size_percent);
    
    // Slow pointer implementation  
    void slow_pointer_validate(const std::string& query, double fast_result);
    
    // Statistical validation
    bool is_approximation_stable(double fast_value, const std::vector<double>& slow_samples);
    double calculate_confidence(const std::vector<double>& samples);
    
    std::string db_path_;
    double error_threshold_;
    int num_threads_;
    int num_fast_threads_;
    std::atomic<bool> stop_flag_;
    std::atomic<ApproximationStatus> current_status_;
    std::atomic<double> current_confidence_;
    
    // Database access
    std::unique_ptr<DirectDBReader> direct_reader_;
    
    // Thread management
    std::vector<std::unique_ptr<std::thread>> fast_threads_;
    std::unique_ptr<std::thread> slow_thread_;
    
    // Shared state for multi-fast results
    std::vector<double> fast_results_;
    std::mutex fast_results_mutex_;
    std::atomic<double> combined_fast_result_;
    
    std::vector<double> slow_samples_;
    mutable std::mutex slow_samples_mutex_;
};