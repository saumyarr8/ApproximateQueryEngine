#include "executor.h"
#include "../core/db.hpp"
#include "parser.h"
#include <stdexcept>
#include <thread>
#include <mutex>
#include <vector>
#include <sstream>
#include <cmath>
#include <algorithm>
#include <cctype>

// Simple parser for the MVP - remove duplicate, use the one from parser.h

static std::string up(const std::string &s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(), ::toupper);
    return r;
}

static int sample_step(int sample_percent) {
    if (sample_percent <= 0 || sample_percent >= 100) return 0;
    int step = 100 / sample_percent;
    if (step <= 0) step = 1;
    return step;
}

double execute_query(const std::string &sql_query, const std::string &db_path, int sample_percent) {
    Query q = parse_query(sql_query, sample_percent);
    DB db(db_path);

    std::ostringstream sql;
    sql << "SELECT " << q.agg << "(" << q.column << ") FROM " << q.table;
    if (!q.where.empty()) sql << " WHERE " << q.where;

    int step = sample_step(sample_percent);
    if (step > 0) {
        if (q.where.empty()) sql << " WHERE ";
        else sql << " AND ";
        sql << "rowid % " << step << " = 0";
    }

    auto results = db.execute_query(sql.str());
    double result = 0.0;
    
    if (!results.empty() && !results[0].empty()) {
        result = std::stod(results[0][0]);
        
        if (step > 0) {
            std::string agg_upper = up(q.agg);
            if (agg_upper != "AVG") {
                result = result * (100.0 / sample_percent);
            }
        }
    }

    return result;
}

GroupResult execute_query_groupby(const std::string &sql_query, const std::string &db_path, 
                                 int sample_percent, int num_threads) {
    Query q = parse_query(sql_query, sample_percent);
    if (q.group_by.empty()) throw std::runtime_error("No GROUP BY column found");

    DB db(db_path);

    // Get distinct group keys
    std::ostringstream groups_sql;
    groups_sql << "SELECT DISTINCT " << q.group_by << " FROM " << q.table;
    if (!q.where.empty()) groups_sql << " WHERE " << q.where;

    auto group_rows = db.execute_query(groups_sql.str());
    std::vector<std::string> groups;
    
    for (const auto& row : group_rows) {
        if (!row.empty()) {
            groups.push_back(row[0]);
        }
    }

    GroupResult final;
    std::mutex mtx;
    int step = sample_step(sample_percent);
    
    auto worker = [&](int start, int end) {
        DB tdb(db_path);
        for (int i = start; i < end; ++i) {
            const std::string &gval = groups[i];
            std::ostringstream qsql;
            qsql << "SELECT " << q.agg << "(" << q.column << ") FROM " << q.table
                 << " WHERE " << q.group_by << " = '" << gval << "'";
            if (!q.where.empty()) qsql << " AND " << q.where;
            if (step > 0) qsql << " AND rowid % " << step << " = 0";

            auto agg_results = tdb.execute_query(qsql.str());
            double agg_val = 0.0;
            
            if (!agg_results.empty() && !agg_results[0].empty()) {
                agg_val = std::stod(agg_results[0][0]);
                std::string agg_upper = up(q.agg);
                if (step > 0 && agg_upper != "AVG") {
                    agg_val *= (100.0 / sample_percent);
                }
            }

            {
                std::lock_guard<std::mutex> lock(mtx);
                final[gval] = agg_val;
            }
        }
    };

    int total = (int)groups.size();
    if (total == 0) return final;
    
    int chunk = (total + num_threads - 1) / num_threads;
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        int start = t * chunk;
        int end = std::min(start + chunk, total);
        if (start >= end) break;
        threads.emplace_back(worker, start, end);
    }
    for (auto &th : threads) th.join();

    return final;
}

// Implementing confidence interval functions
QueryResult execute_query_with_ci(const std::string &sql_query, const std::string &db_path, int sample_percent) {
    Query q = parse_query(sql_query, sample_percent);
    DB db(db_path);
    
    int step = sample_step(sample_percent);
    
    // If exact query or not appropriate for CI calculation, use regular execution
    if (step == 0 || (up(q.agg) != "SUM" && up(q.agg) != "AVG")) {
        double result = execute_query(sql_query, db_path, sample_percent);
        return {result, result, result}; // No CI for exact queries
    }
    
    std::ostringstream sql;
    
    // First get count, sum, sum of squares for CI calculation
    sql << "SELECT COUNT(" << q.column << "), SUM(" << q.column << "), "
        << "SUM(" << q.column << " * " << q.column << ") "
        << "FROM " << q.table;
    
    if (!q.where.empty()) {
        sql << " WHERE " << q.where;
    }
    
    // Add sampling condition
    if (step > 0) {
        if (q.where.empty()) sql << " WHERE ";
        else sql << " AND ";
        sql << "rowid % " << step << " = 0";
    }
    
    auto results = db.execute_query(sql.str());
    if (results.empty() || results[0].size() < 3) {
        double result = execute_query(sql_query, db_path, sample_percent);
        return {result, result, result};
    }
    
    double count = std::stod(results[0][0]);
    double sum = std::stod(results[0][1]);
    double sum_sq = std::stod(results[0][2]);
    
    // Calculate statistics
    if (count < 2) {
        // Not enough data for CI
        double result = execute_query(sql_query, db_path, sample_percent);
        return {result, result, result};
    }
    
    double mean, variance, std_error, margin;
    
    // Calculate mean
    mean = sum / count;
    
    // Calculate variance: var = (sum_sq - (sum^2 / n)) / (n-1)
    variance = (sum_sq - (sum * sum / count)) / (count - 1);
    
    // Standard error of the mean
    std_error = sqrt(variance / count);
    
    // Margin of error (95% CI)
    margin = 1.96 * std_error;
    
    // Scale for SUM
    double scale_factor = 1.0;
    if (up(q.agg) == "SUM") {
        scale_factor = 100.0 / sample_percent;
        mean *= scale_factor;
        margin *= scale_factor;
    }
    
    return {mean, mean - margin, mean + margin};
}

GroupResultWithCI execute_query_groupby_with_ci(const std::string &sql_query, const std::string &db_path, 
                                              int sample_percent, int num_threads) {
    Query q = parse_query(sql_query, sample_percent);
    if (q.group_by.empty()) throw std::runtime_error("No GROUP BY column found");
    
    DB db(db_path);
    
    // Get distinct group keys
    std::ostringstream groups_sql;
    groups_sql << "SELECT DISTINCT " << q.group_by << " FROM " << q.table;
    if (!q.where.empty()) groups_sql << " WHERE " << q.where;
    
    auto group_rows = db.execute_query(groups_sql.str());
    std::vector<std::string> groups;
    
    for (const auto& row : group_rows) {
        if (!row.empty()) {
            groups.push_back(row[0]);
        }
    }
    
    GroupResultWithCI final;
    std::mutex mtx;
    int step = sample_step(sample_percent);
    std::string agg_upper = up(q.agg);
    
    auto worker = [&](int start, int end) {
        DB tdb(db_path);
        
        for (int i = start; i < end; ++i) {
            const std::string &gval = groups[i];
            
            // For CI calculation we need count, sum, sum of squares
            std::ostringstream qsql;
            qsql << "SELECT COUNT(" << q.column << "), SUM(" << q.column << "), "
                 << "SUM(" << q.column << " * " << q.column << ") "
                 << "FROM " << q.table
                 << " WHERE " << q.group_by << " = '" << gval << "'";
            
            if (!q.where.empty()) qsql << " AND " << q.where;
            if (step > 0) qsql << " AND rowid % " << step << " = 0";
            
            auto stats_rows = tdb.execute_query(qsql.str());
            if (stats_rows.empty() || stats_rows[0].size() < 3) continue;
            
            double count = std::stod(stats_rows[0][0]);
            double sum = std::stod(stats_rows[0][1]);
            double sum_sq = std::stod(stats_rows[0][2]);
            
            // Calculate statistics
            double mean, variance, std_error, margin;
            double scale_factor = 1.0;
            
            if (count < 2) {
                // Not enough data for CI, fall back to regular calculation
                std::ostringstream simple_sql;
                simple_sql << "SELECT " << q.agg << "(" << q.column << ") FROM " << q.table
                           << " WHERE " << q.group_by << " = '" << gval << "'";
                if (!q.where.empty()) simple_sql << " AND " << q.where;
                if (step > 0) simple_sql << " AND rowid % " << step << " = 0";
                
                auto value_rows = tdb.execute_query(simple_sql.str());
                double value = 0.0;
                if (!value_rows.empty() && !value_rows[0].empty()) {
                    value = std::stod(value_rows[0][0]);
                    if (step > 0 && agg_upper != "AVG") {
                        value *= (100.0 / sample_percent);
                    }
                }
                
                {
                    std::lock_guard<std::mutex> lock(mtx);
                    final[gval] = {value, value, value}; // No CI
                }
                continue;
            }
            
            // Calculate mean
            mean = sum / count;
            
            // Calculate variance
            variance = (sum_sq - (sum * sum / count)) / (count - 1);
            
            // Standard error of the mean
            std_error = sqrt(variance / count);
            
            // Margin of error (95% CI)
            margin = 1.96 * std_error;
            
            // Scale for SUM
            if (agg_upper == "SUM") {
                scale_factor = 100.0 / sample_percent;
                mean *= scale_factor;
                margin *= scale_factor;
            }
            
            {
                std::lock_guard<std::mutex> lock(mtx);
                final[gval] = {mean, mean - margin, mean + margin};
            }
        }
    };
    
    int total = (int)groups.size();
    if (total == 0) return final;
    
    int chunk = (total + num_threads - 1) / num_threads;
    std::vector<std::thread> threads;
    
    for (int t = 0; t < num_threads; ++t) {
        int start = t * chunk;
        int end = std::min(start + chunk, total);
        if (start >= end) break;
        threads.emplace_back(worker, start, end);
    }
    
    for (auto &th : threads) th.join();
    
    return final;
}