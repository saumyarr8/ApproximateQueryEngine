#pragma once

#include "custom_bplus_db.hpp"
#include <string>
#include <memory>
#include <chrono>

enum class CustomApproximationStatus {
    STABLE,
    DRIFTING,
    INSUFFICIENT_DATA,
    ERROR
};

struct CustomValidationResult {
    double value;
    CustomApproximationStatus status;
    double confidence_level;
    double error_margin;
    int samples_used;
    std::chrono::milliseconds computation_time;
};

/**
 * High-performance approximate query scheduler using custom B+ tree database.
 * Optimized for true parallel processing without SQLite overhead.
 */
class CustomApproximateScheduler {
public:
    CustomApproximateScheduler(double error_threshold = 0.05);
    ~CustomApproximateScheduler();
    
    // Database management
    bool create_database(const std::string& db_path);
    bool open_database(const std::string& db_path);
    void close_database();
    
    // Data insertion
    bool insert_record(int64_t id, double amount, int32_t region, 
                      int32_t product_id, int64_t timestamp);
    bool insert_batch(const std::vector<Record>& records);
    
    // Query execution methods
    CustomValidationResult execute_sum_query(const std::string& query, 
                                            double sample_percent = 10.0,
                                            int num_threads = 4);
    
    CustomValidationResult execute_avg_query(const std::string& query,
                                            double sample_percent = 10.0, 
                                            int num_threads = 4);
    
    CustomValidationResult execute_count_query(const std::string& query,
                                              double sample_percent = 10.0,
                                              int num_threads = 4);
    
    CustomValidationResult execute_sum_where_query(const std::string& query,
                                                  double min_amount, double max_amount,
                                                  double sample_percent = 10.0,
                                                  int num_threads = 4);
    
    // Exact queries for benchmarking
    CustomValidationResult execute_exact_sum();
    CustomValidationResult execute_exact_avg();
    CustomValidationResult execute_exact_count();
    CustomValidationResult execute_exact_sum_where(double min_amount, double max_amount);
    
    // Database statistics
    size_t get_total_records() const;
    size_t get_tree_height() const;
    double get_database_size_mb() const;
    
    // Performance comparison
    struct BenchmarkResults {
        double exact_value;
        double approximate_value;
        double exact_time_ms;
        double approximate_time_ms;
        double speedup;
        double error_percentage;
        int threads_used;
        double sample_percentage;
    };
    
    BenchmarkResults benchmark_query(const std::string& query_type,
                                    double sample_percent = 10.0,
                                    int num_threads = 4);
    
private:
    std::unique_ptr<CustomBPlusDB> db_;
    double error_threshold_;
    std::string db_path_;
    
    // Query parsing helpers
    enum class QueryType {
        SUM,
        AVG, 
        COUNT,
        SUM_WHERE,
        UNKNOWN
    };
    
    QueryType parse_query_type(const std::string& query);
    std::pair<double, double> extract_where_conditions(const std::string& query);
    
    // Result validation
    bool validate_approximation_quality(double exact_value, double approx_value,
                                       double error_threshold);
    double calculate_confidence_level(double sample_percent, size_t total_records);
};