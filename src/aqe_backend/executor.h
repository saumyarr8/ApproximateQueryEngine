#pragma once
#include <string>
#include <map>

using GroupResult = std::map<std::string, double>;

// Return type for queries with confidence intervals
struct QueryResult {
    double value;
    double ci_lower;
    double ci_upper;
};

using GroupResultWithCI = std::map<std::string, QueryResult>;

double execute_query(const std::string &sql_query, const std::string &db_path, int sample_percent=0);
GroupResult execute_query_groupby(const std::string &sql_query, const std::string &db_path, 
                                 int sample_percent=0, int num_threads=4);

// Functions with confidence intervals
QueryResult execute_query_with_ci(const std::string &sql_query, const std::string &db_path, int sample_percent=0);
GroupResultWithCI execute_query_groupby_with_ci(const std::string &sql_query, const std::string &db_path, 
                                              int sample_percent=0, int num_threads=4);