#include "parser.h"
#include <algorithm>
#include <stdexcept>
#include <cctype>

static std::string up(const std::string &s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(), ::toupper);
    return r;
}

static std::string trim(std::string s) {
    const char *ws = " \t\n\r";
    size_t a = s.find_first_not_of(ws);
    size_t b = s.find_last_not_of(ws);
    if (a==std::string::npos) return "";
    return s.substr(a, b - a + 1);
}

Query parse_query(const std::string &sql, int sample_percent) {
    Query q;
    q.sample_percent = sample_percent;
    q.where = "";
    q.group_by = "";

    std::string U = up(sql);

    size_t sel = U.find("SELECT");
    size_t from = U.find("FROM");
    if (sel == std::string::npos || from == std::string::npos)
        throw std::runtime_error("Invalid SQL: missing SELECT or FROM");

    std::string agg_col = sql.substr(sel + 6, from - (sel + 6));
    agg_col = trim(agg_col);

    size_t where_pos = U.find("WHERE");
    size_t group_pos = U.find("GROUP BY");

    if (where_pos != std::string::npos) {
        q.table = trim(sql.substr(from + 4, where_pos - (from + 4)));
        if (group_pos != std::string::npos) {
            q.where = trim(sql.substr(where_pos + 5, group_pos - (where_pos + 5)));
            q.group_by = trim(sql.substr(group_pos + 8));
        } else {
            q.where = trim(sql.substr(where_pos + 5));
        }
    } else if (group_pos != std::string::npos) {
        q.table = trim(sql.substr(from + 4, group_pos - (from + 4)));
        q.group_by = trim(sql.substr(group_pos + 8));
    } else {
        q.table = trim(sql.substr(from + 4));
    }

    // remove trailing semicolon in table/group_by/where if present
    if (!q.table.empty() && q.table.back()==';') q.table.pop_back();
    if (!q.group_by.empty() && q.group_by.back()==';') q.group_by.pop_back();
    if (!q.where.empty() && q.where.back()==';') q.where.pop_back();

    // parse agg(col) - supports SUM, COUNT, AVG with proper sampling scaling
    size_t paren_open = agg_col.find("(");
    size_t paren_close = agg_col.find(")");
    if (paren_open==std::string::npos || paren_close==std::string::npos)
        throw std::runtime_error("Invalid aggregation syntax");

    q.agg = trim(agg_col.substr(0, paren_open));
    q.column = trim(agg_col.substr(paren_open+1, paren_close - paren_open - 1));

    // Validate supported aggregation functions
    std::string agg_upper = up(q.agg);
    if (agg_upper != "SUM" && agg_upper != "COUNT" && agg_upper != "AVG") {
        throw std::runtime_error("Unsupported aggregation function: " + q.agg + 
                                ". Supported functions: SUM, COUNT, AVG");
    }

    return q;
}