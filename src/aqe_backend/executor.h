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
                                              int sample_percent=0, int num_threads=4);                                            # filepath: /home/sam/ApproximateQueryEngine/test_module.py
                                            import sys
                                            import os
                                            
                                            # Add the build directory to Python path
                                            build_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'build')
                                            sys.path.insert(0, build_path)
                                            
                                            try:
                                                import aqe_backend
                                                print("Successfully imported aqe_backend module!")
                                                print("Available functions:", dir(aqe_backend))
                                            except ImportError as e:
                                                print(f"Error importing module: {e}")