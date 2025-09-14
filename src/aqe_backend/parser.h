#pragma once
#include <string>

/**
 * Query structure representing a parsed SQL query with sampling support.
 * 
 * Supports the following aggregation functions:
 * - SUM(column): Returns sum of sampled values, scaled by sampling factor
 *   Example: If sampling 10% and sampled sum is 1000, returns 10000
 * - COUNT(column): Returns count of sampled rows, scaled by sampling factor  
 *   Example: If sampling 10% and sampled count is 100, returns 1000
 * - AVG(column): Returns average of sampled values, no scaling needed
 *   Example: If sampling 10% and sampled average is 50, returns 50
 * 
 * Scaling formula for SUM and COUNT: result * (100 / sample_percent)
 */
struct Query {
    std::string agg;           // Aggregation function: SUM, COUNT, or AVG
    std::string column;        // Column to aggregate
    std::string table;         // Table name
    std::string where;         // WHERE clause (optional)
    int sample_percent;        // Sampling percentage (1-100, 0 = exact)
    std::string group_by;      // GROUP BY column (optional)
};

/**
 * Parse SQL query string into Query structure.
 * 
 * Supported SQL syntax:
 * - SELECT SUM(column) FROM table
 * - SELECT COUNT(column) FROM table  
 * - SELECT AVG(column) FROM table
 * - All above with optional WHERE and GROUP BY clauses
 * 
 * @param sql SQL query string
 * @param sample_percent Sampling percentage (0 = exact query)
 * @return Parsed Query structure
 * @throws std::runtime_error for invalid syntax or unsupported functions
 */
Query parse_query(const std::string &sql, int sample_percent);