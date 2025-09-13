"""
Command-line interface for AQE.
"""
import argparse
from .runner import run_query


def main():
    parser = argparse.ArgumentParser(description="Approximate Query Engine CLI")
    parser.add_argument("query", type=str, help="SQL-like query, e.g., 'SELECT SUM(value) FROM mytable;'")
    parser.add_argument("--db", type=str, default="example.db", help="Path to SQLite database")

    args = parser.parse_args()

    try:
        result = run_query(args.query, args.db)
        print("Result:", result)
    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    main()
