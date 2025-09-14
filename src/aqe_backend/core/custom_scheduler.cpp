#include "custom_scheduler.hpp"
#include <algorithm>
#include <regex>
#include <iostream>
#include <sstream>

CustomApproximateScheduler::CustomApproximateScheduler(double error_threshold) 
    : error_threshold_(error_threshold) {
    db_ = std::make_unique<CustomBPlusDB>();
}

CustomApproximateScheduler::~CustomApproximateScheduler() {
    close_database();
}

bool CustomApproximateScheduler::create_database(const std::string& db_path) {
    db_path_ = db_path;
    return db_->create_database(db_path);
}

bool CustomApproximateScheduler::open_database(const std::string& db_path) {
    db_path_ = db_path;
    return db_->open_database(db_path);
}

void CustomApproximateScheduler::close_database() {
    if (db_) {
        db_->close_database();
    }
}

bool CustomApproximateScheduler::insert_record(int64_t id, double amount, int32_t region,
                                              int32_t product_id, int64_t timestamp) {
    Record record(id, amount, region, product_id, timestamp);
    return db_->insert_record(record);
}

bool CustomApproximateScheduler::insert_batch(const std::vector<Record>& records) {
    return db_->insert_batch(records);
}

CustomValidationResult CustomApproximateScheduler::execute_sum_query(const std::string& query,
                                                                    double sample_percent,
                                                                    int num_threads) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    CustomValidationResult result;
    result.status = CustomApproximationStatus::ERROR;
    result.confidence_level = 0.0;
    result.error_margin = 100.0;
    result.samples_used = 0;
    
    try {
        // Parse query for WHERE conditions
        auto where_conditions = extract_where_conditions(query);
        
        double sum_result;
        if (where_conditions.first != -1 && where_conditions.second != -1) {
            // SUM with WHERE clause
            sum_result = db_->parallel_sum_where_sample(where_conditions.first, 
                                                       where_conditions.second,
                                                       sample_percent, num_threads);
        } else {
            // Simple SUM
            sum_result = db_->parallel_sum_sample(sample_percent, num_threads);
        }
        
        result.value = sum_result;
        result.status = CustomApproximationStatus::STABLE;
        result.confidence_level = calculate_confidence_level(sample_percent, db_->get_total_records());
        result.error_margin = sample_percent / 100.0;
        result.samples_used = static_cast<int>(db_->get_total_records() * sample_percent / 100.0);
        
    } catch (const std::exception& e) {
        result.value = 0.0;
        result.status = CustomApproximationStatus::ERROR;
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    result.computation_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    return result;
}

CustomValidationResult CustomApproximateScheduler::execute_avg_query(const std::string& query,
                                                                    double sample_percent,
                                                                    int num_threads) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    CustomValidationResult result;
    result.status = CustomApproximationStatus::ERROR;
    
    try {
        double avg_result = db_->parallel_avg_sample(sample_percent, num_threads);
        
        result.value = avg_result;
        result.status = CustomApproximationStatus::STABLE;
        result.confidence_level = calculate_confidence_level(sample_percent, db_->get_total_records());
        result.error_margin = sample_percent / 100.0;
        result.samples_used = static_cast<int>(db_->get_total_records() * sample_percent / 100.0);
        
    } catch (const std::exception& e) {
        result.value = 0.0;
        result.status = CustomApproximationStatus::ERROR;
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    result.computation_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    return result;
}

CustomValidationResult CustomApproximateScheduler::execute_count_query(const std::string& query,
                                                                      double sample_percent,
                                                                      int num_threads) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    CustomValidationResult result;
    result.status = CustomApproximationStatus::ERROR;
    
    try {
        size_t count_result = db_->parallel_count_sample(sample_percent, num_threads);
        
        result.value = static_cast<double>(count_result);
        result.status = CustomApproximationStatus::STABLE;
        result.confidence_level = calculate_confidence_level(sample_percent, db_->get_total_records());
        result.error_margin = sample_percent / 100.0;
        result.samples_used = static_cast<int>(db_->get_total_records() * sample_percent / 100.0);
        
    } catch (const std::exception& e) {
        result.value = 0.0;
        result.status = CustomApproximationStatus::ERROR;
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    result.computation_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    return result;
}

CustomValidationResult CustomApproximateScheduler::execute_exact_sum() {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    CustomValidationResult result;
    result.status = CustomApproximationStatus::STABLE;
    result.confidence_level = 1.0;
    result.error_margin = 0.0;
    result.samples_used = static_cast<int>(db_->get_total_records());
    
    try {
        result.value = db_->sum_amount();
    } catch (const std::exception& e) {
        result.value = 0.0;
        result.status = CustomApproximationStatus::ERROR;
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    result.computation_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    return result;
}

CustomValidationResult CustomApproximateScheduler::execute_exact_avg() {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    CustomValidationResult result;
    result.status = CustomApproximationStatus::STABLE;
    result.confidence_level = 1.0;
    result.error_margin = 0.0;
    result.samples_used = static_cast<int>(db_->get_total_records());
    
    try {
        result.value = db_->avg_amount();
    } catch (const std::exception& e) {
        result.value = 0.0;
        result.status = CustomApproximationStatus::ERROR;
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    result.computation_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    return result;
}

CustomValidationResult CustomApproximateScheduler::execute_exact_count() {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    CustomValidationResult result;
    result.status = CustomApproximationStatus::STABLE;
    result.confidence_level = 1.0;
    result.error_margin = 0.0;
    result.samples_used = static_cast<int>(db_->get_total_records());
    
    try {
        result.value = static_cast<double>(db_->count_records());
    } catch (const std::exception& e) {
        result.value = 0.0;
        result.status = CustomApproximationStatus::ERROR;
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    result.computation_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    return result;
}

CustomApproximateScheduler::BenchmarkResults CustomApproximateScheduler::benchmark_query(
    const std::string& query_type, double sample_percent, int num_threads) {
    
    BenchmarkResults results;
    results.sample_percentage = sample_percent;
    results.threads_used = num_threads;
    
    CustomValidationResult exact_result, approx_result;
    
    if (query_type == "SUM") {
        exact_result = execute_exact_sum();
        approx_result = execute_sum_query("SELECT SUM(amount)", sample_percent, num_threads);
    } else if (query_type == "AVG") {
        exact_result = execute_exact_avg();
        approx_result = execute_avg_query("SELECT AVG(amount)", sample_percent, num_threads);
    } else if (query_type == "COUNT") {
        exact_result = execute_exact_count();
        approx_result = execute_count_query("SELECT COUNT(*)", sample_percent, num_threads);
    } else {
        // Default to SUM
        exact_result = execute_exact_sum();
        approx_result = execute_sum_query("SELECT SUM(amount)", sample_percent, num_threads);
    }
    
    results.exact_value = exact_result.value;
    results.approximate_value = approx_result.value;
    results.exact_time_ms = exact_result.computation_time.count();
    results.approximate_time_ms = approx_result.computation_time.count();
    results.speedup = results.exact_time_ms / results.approximate_time_ms;
    
    if (exact_result.value != 0) {
        results.error_percentage = std::abs(exact_result.value - approx_result.value) / 
                                  std::abs(exact_result.value) * 100.0;
    } else {
        results.error_percentage = 0.0;
    }
    
    return results;
}

size_t CustomApproximateScheduler::get_total_records() const {
    return db_->get_total_records();
}

size_t CustomApproximateScheduler::get_tree_height() const {
    return db_->get_tree_height();
}

double CustomApproximateScheduler::get_database_size_mb() const {
    return get_total_records() * sizeof(Record) / (1024.0 * 1024.0);
}

CustomApproximateScheduler::QueryType CustomApproximateScheduler::parse_query_type(const std::string& query) {
    std::string upper_query = query;
    std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
    
    if (upper_query.find("SUM(") != std::string::npos) {
        if (upper_query.find("WHERE") != std::string::npos) {
            return QueryType::SUM_WHERE;
        }
        return QueryType::SUM;
    } else if (upper_query.find("AVG(") != std::string::npos) {
        return QueryType::AVG;
    } else if (upper_query.find("COUNT(") != std::string::npos) {
        return QueryType::COUNT;
    }
    
    return QueryType::UNKNOWN;
}

std::pair<double, double> CustomApproximateScheduler::extract_where_conditions(const std::string& query) {
    // Simple WHERE amount BETWEEN x AND y parser
    std::regex between_regex(R"(amount\s+BETWEEN\s+(\d+(?:\.\d+)?)\s+AND\s+(\d+(?:\.\d+)?))");
    std::regex greater_regex(R"(amount\s*>\s*(\d+(?:\.\d+)?))");
    std::regex range_regex(R"(amount\s*>=\s*(\d+(?:\.\d+)?)\s+AND\s+amount\s*<=\s*(\d+(?:\.\d+)?))");
    
    std::smatch match;
    
    if (std::regex_search(query, match, between_regex)) {
        return {std::stod(match[1]), std::stod(match[2])};
    } else if (std::regex_search(query, match, range_regex)) {
        return {std::stod(match[1]), std::stod(match[2])};
    } else if (std::regex_search(query, match, greater_regex)) {
        return {std::stod(match[1]), 99999.99}; // Default upper bound
    }
    
    return {-1, -1}; // No WHERE conditions found
}

double CustomApproximateScheduler::calculate_confidence_level(double sample_percent, size_t total_records) {
    // Simple confidence calculation based on sample size
    double sample_size = total_records * sample_percent / 100.0;
    
    if (sample_size >= 1000) return 0.95;
    if (sample_size >= 500) return 0.90;
    if (sample_size >= 100) return 0.85;
    if (sample_size >= 50) return 0.80;
    return 0.70;
}

bool CustomApproximateScheduler::validate_approximation_quality(double exact_value, double approx_value,
                                                               double error_threshold) {
    if (exact_value == 0) return approx_value == 0;
    
    double error = std::abs(exact_value - approx_value) / std::abs(exact_value);
    return error <= error_threshold;
}