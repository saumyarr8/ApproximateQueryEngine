"""
Very simple SQL-like query parser
Currently supports:
    SELECT SUM(column) FROM table;
    SELECT COUNT(column) FROM table;
    SELECT AVG(column) FROM table;
"""

import re

def parse_query(query: str):
    query = query.strip().rstrip(";").upper()

    # Regex for SELECT SUM(value) FROM mytable
    match = re.match(r"SELECT\s+(SUM|COUNT|AVG)\((\w+)\)\s+FROM\s+(\w+)", query)
    if not match:
        raise ValueError(f"Unsupported query: {query}")

    agg_func, column, table = match.groups()
    return {
        "agg_func": agg_func,
        "column": column,
        "table": table,
    }
