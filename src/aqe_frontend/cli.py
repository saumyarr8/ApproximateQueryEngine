"""
Command line interface for the Approximate Query Engine.
"""

import argparse
import time
import sys
from .runner import run_query, run_query_groupby, run_query_with_ci, run_query_groupby_with_ci

def main():
    parser = argparse.ArgumentParser(description="Approximate Query Engine CLI")
    parser.add_argument("query", help="SQL query to execute")
    parser.add_argument("--db", default="example.db", help="Database file path")
    parser.add_argument("--sample", type=int, default=0, help="Sample percentage (0 for exact)")
    parser.add_argument("--threads", type=int, default=4, help="Number of threads for GROUP BY")
    parser.add_argument("--ci", action="store_true", help="Calculate confidence intervals")
    args = parser.parse_args()

    try:
        start_time = time.time()
        
        # Check if query contains GROUP BY
        if "GROUP BY" in args.query.upper() or "group by" in args.query.lower():
            if args.ci:
                result = run_query_groupby_with_ci(args.query, args.db, args.sample, args.threads)
                print("\nResults with 95% confidence intervals:")
                for group, res in result.items():
                    print(f"  {group}: {res.value:.2f} ({res.ci_lower:.2f} - {res.ci_upper:.2f})")
            else:
                result = run_query_groupby(args.query, args.db, args.sample, args.threads)
                print("\nResults:")
                for group, value in result.items():
                    print(f"  {group}: {value}")
        else:
            if args.ci:
                result = run_query_with_ci(args.query, args.db, args.sample)
                print(f"Result: {result.value:.2f} ({result.ci_lower:.2f} - {result.ci_upper:.2f})")
            else:
                result = run_query(args.query, args.db, args.sample)
                print(f"Result: {result}")
        
        elapsed = time.time() - start_time
        print(f"\nQuery completed in {elapsed:.3f} seconds")
        
        if args.sample > 0:
            print(f"Using approximate sampling: {args.sample}%")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())