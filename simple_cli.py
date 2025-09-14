#!/usr/bin/env python3
"""
Simple CLI for the Approximate Query Engine
"""
import sys
import argparse
import time
import importlib.util

def import_aqe_backend():
    """Import the aqe_backend module directly"""
    spec = importlib.util.spec_from_file_location('aqe_backend', './src/aqe_backend.cpython-312-x86_64-linux-gnu.so')
    aqe_backend = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(aqe_backend)
    return aqe_backend

def main():
    parser = argparse.ArgumentParser(description='Approximate Query Engine CLI')
    parser.add_argument('query', help='SQL query to execute')
    parser.add_argument('--db', default='large_sales_1m.db', help='Database file path')
    parser.add_argument('--s', '--sample', type=int, default=0, help='Sample percentage (0=exact)')
    parser.add_argument('--threads', type=int, default=4, help='Number of threads for GROUP BY')
    parser.add_argument('--ci', action='store_true', help='Show confidence intervals')
    
    args = parser.parse_args()
    
    try:
        # Import backend
        aqe_backend = import_aqe_backend()
        
        print(f"Query: {args.query}")
        print(f"Database: {args.db}")
        print(f"Sample rate: {args.s}% {'(exact)' if args.s == 0 else '(approximate)'}")
        print("-" * 60)
        
        start_time = time.time()
        
        # Check if it's a GROUP BY query
        if 'GROUP BY' in args.query.upper():
            if args.ci:
                result = aqe_backend.run_query_groupby_with_ci(args.query, args.db, args.s, args.threads)
                print("Results with confidence intervals:")
                for key, value in result.items():
                    print(f"  {key}: {value.value:.2f} [{value.ci_lower:.2f}, {value.ci_upper:.2f}]")
            else:
                result = aqe_backend.run_query_groupby(args.query, args.db, args.s, args.threads)
                print("Results:")
                for key, value in result.items():
                    print(f"  {key}: {value:.2f}")
        else:
            if args.ci:
                result = aqe_backend.run_query_with_ci(args.query, args.db, args.s)
                print(f"Result: {result.value:.2f}")
                print(f"95% Confidence Interval: [{result.ci_lower:.2f}, {result.ci_upper:.2f}]")
            else:
                result = aqe_backend.run_query(args.query, args.db, args.s)
                print(f"Result: {result:.2f}")
        
        elapsed = time.time() - start_time
        print(f"Execution time: {elapsed*1000:.1f}ms")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())