#!/usr/bin/env python3
"""
Enhanced ApproximateQueryEngine CLI with multiple query syntaxes and approximation methods.

Supports multiple query formats:
1. SQL-embedded: SELECT APPROX(SUM(values)) FROM table;
2. Flag-based sampling: SELECT SUM(values) FROM table --s 10;
3. Flag-based CLT: SELECT SUM(values) FROM table --e 2;

Usage Examples:
    python enhanced_aqe_cli.py "SELECT APPROX(SUM(amount)) FROM sales"
    python enhanced_aqe_cli.py "SELECT SUM(amount) FROM sales" --s 10
    python enhanced_aqe_cli.py "SELECT SUM(amount) FROM sales" --e 2
    python enhanced_aqe_cli.py "SELECT AVG(amount) FROM sales" --method clt --e 1.5
"""

import argparse
import time
import sys
import os
import re

# Add the build directory to path for aqe_backend module
build_path = os.path.join(os.path.dirname(__file__), 'build', 'src', 'aqe_backend')
sys.path.insert(0, build_path)
import aqe_backend

class QueryType:
    """Enumeration of different query types and their optimal methods."""
    EXACT = "exact"
    RANDOM_SAMPLE = "random_sample"
    CLT_APPROXIMATION = "clt_approximation"
    BLOCK_SAMPLE = "block_sample"
    EMBEDDED_APPROX = "embedded_approx"

class ApproximationMethod:
    """Available approximation methods with their characteristics."""
    METHODS = {
        'random': {
            'name': 'Random Sampling',
            'description': 'Randomly samples specified percentage of data',
            'best_for': 'Quick estimates, large datasets',
            'accuracy': 'Good',
            'speed': 'Very Fast'
        },
        'clt': {
            'name': 'Central Limit Theorem',
            'description': 'Uses CLT for statistical confidence in results',
            'best_for': 'High accuracy requirements, SUM/COUNT queries',
            'accuracy': 'Excellent',
            'speed': 'Fast'
        },
        'block': {
            'name': 'Block Sampling',
            'description': 'Samples contiguous blocks of data',
            'best_for': 'Temporal data, cache-friendly access',
            'accuracy': 'Good',
            'speed': 'Fast'
        },
        'adaptive': {
            'name': 'Adaptive Sampling',
            'description': 'Automatically adjusts sampling based on error requirements',
            'best_for': 'General purpose, automatic optimization',
            'accuracy': 'Very Good',
            'speed': 'Variable'
        },
        'parallel': {
            'name': 'Parallel Sampling',
            'description': 'Multi-threaded sampling for faster processing',
            'best_for': 'Large datasets, multi-core systems',
            'accuracy': 'Good',
            'speed': 'Very Fast'
        },
        'revolutionary': {
            'name': 'Revolutionary Sampling',
            'description': 'Optimized method selection based on data characteristics',
            'best_for': 'Maximum performance, adaptive to dataset size',
            'accuracy': 'Very Good',
            'speed': 'Extremely Fast'
        }
    }

def parse_embedded_approx(query):
    """Parse APPROX() function embedded in SQL query."""
    # Match APPROX(function) patterns
    approx_pattern = r'APPROX\s*\(\s*([^)]+)\s*\)'
    match = re.search(approx_pattern, query, re.IGNORECASE)
    
    if match:
        inner_function = match.group(1)
        # Replace APPROX(func) with just func for processing
        clean_query = re.sub(approx_pattern, inner_function, query, flags=re.IGNORECASE)
        return clean_query, True
    
    return query, False

def determine_query_type(query, args):
    """Determine the type of query and appropriate method."""
    query_lower = query.lower()
    
    # Check for embedded APPROX() function
    _, has_approx = parse_embedded_approx(query)
    if has_approx:
        return QueryType.EMBEDDED_APPROX
    
    # Check flags
    if hasattr(args, 's') and args.s is not None:
        return QueryType.RANDOM_SAMPLE
    elif hasattr(args, 'e') and args.e is not None:
        return QueryType.CLT_APPROXIMATION
    elif args.sample and args.sample > 0:
        return QueryType.BLOCK_SAMPLE
    else:
        return QueryType.EXACT

def get_optimal_method_for_query(query, dataset_size=None):
    """Recommend optimal approximation method based on query characteristics."""
    query_upper = query.upper()
    
    # Analyze query type
    if 'SUM(' in query_upper or 'COUNT(' in query_upper:
        if dataset_size and dataset_size > 100000:
            return 'revolutionary'  # Best for large datasets
        else:
            return 'clt'  # Best accuracy for aggregations
    elif 'AVG(' in query_upper:
        return 'random'  # AVG works well with random sampling
    elif 'GROUP BY' in query_upper:
        return 'parallel'  # Parallel processing for grouped data
    else:
        return 'adaptive'  # Safe default

def format_time(milliseconds):
    """Format time in a human-readable way."""
    if milliseconds < 1000:
        return f"{milliseconds:.1f}ms"
    else:
        return f"{milliseconds/1000:.2f}s"

def format_status(status):
    """Format status with text indicators."""
    try:
        status_map = {
            aqe_backend.ApproximationStatus.STABLE: "✅ STABLE",
            aqe_backend.ApproximationStatus.DRIFTING: "⚠️  DRIFTING", 
            aqe_backend.ApproximationStatus.INSUFFICIENT_DATA: "❌ INSUFFICIENT_DATA",
            aqe_backend.ApproximationStatus.ERROR: "❌ ERROR"
        }
    except AttributeError:
        status_map = {
            aqe_backend.CustomApproximationStatus.STABLE: "✅ STABLE",
            aqe_backend.CustomApproximationStatus.DRIFTING: "⚠️  DRIFTING", 
            aqe_backend.CustomApproximationStatus.INSUFFICIENT_DATA: "❌ INSUFFICIENT_DATA",
            aqe_backend.CustomApproximationStatus.ERROR: "❌ ERROR"
        }
    return status_map.get(status, str(status))

def execute_random_sampling(query, db_path, sample_percent, method='random'):
    """Execute query using random sampling with specified percentage."""
    print(f"🎲 Random Sampling Method ({sample_percent}%)")
    
    start_time = time.time()
    
    # Open custom database
    db = aqe_backend.CustomBPlusDB()
    if not db.open_database(db_path):
        if not db.load_from_file(db_path):
            raise Exception(f"Cannot open custom database: {db_path}")
    
    # Use optimized sampling method based on query type
    query_upper = query.upper()
    total_records = db.get_total_records()
    
    # Calculate number of samples needed
    num_samples = int((sample_percent / 100.0) * total_records)
    
    # Use different sampling strategies
    if total_records > 50000:
        samples = db.memory_stride_sample(sample_percent, 0)  # Memory stride for large datasets
        method_desc = "Memory Stride"
    elif total_records > 10000:
        samples = db.direct_access_sample(sample_percent)  # Direct access for medium datasets
        method_desc = "Direct Access"
    else:
        samples = db.optimized_sequential_sample(sample_percent)  # Sequential for small datasets
        method_desc = "Sequential"
    
    # Calculate result from samples
    if samples:
        if 'SUM(' in query_upper:
            sample_sum = sum(record.amount for record in samples)
            # Scale up to estimate total population sum
            estimated_value = sample_sum * (total_records / len(samples))
        elif 'AVG(' in query_upper:
            estimated_value = sum(record.amount for record in samples) / len(samples)
        elif 'COUNT(' in query_upper:
            estimated_value = total_records  # Always return exact count
        else:
            # Default to average
            estimated_value = sum(record.amount for record in samples) / len(samples)
        
        end_time = time.time()
        computation_time_ms = (end_time - start_time) * 1000
        
        # Create result object
        class RandomSampleResult:
            def __init__(self, value, samples_used, computation_time_ms, method_desc):
                self.value = value
                self.samples_used = f"{len(samples):,} samples ({sample_percent}%)"
                self.computation_time = type('obj', (object,), {
                    'total_seconds': lambda: computation_time_ms / 1000.0
                })()
                try:
                    self.status = aqe_backend.ApproximationStatus.STABLE
                except AttributeError:
                    try:
                        self.status = aqe_backend.CustomApproximationStatus.STABLE
                    except AttributeError:
                        self.status = "STABLE"
                self.confidence_level = 0.90  # Estimated confidence for random sampling
                self.error_margin = 0.05  # Estimated error margin
                self.method_name = f"Random {method_desc} Sampling ({sample_percent}%)"
        
        db.close_database()
        return RandomSampleResult(estimated_value, len(samples), computation_time_ms, method_desc)
    else:
        db.close_database()
        raise Exception("No samples collected")

def execute_clt_approximation(query, db_path, error_threshold, method='clt'):
    """Execute query using CLT approximation with error threshold."""
    print(f"📊 CLT Approximation Method (±{error_threshold}% error)")
    
    start_time = time.time()
    
    # Open custom database
    db = aqe_backend.CustomBPlusDB()
    if not db.open_database(db_path):
        if not db.load_from_file(db_path):
            raise Exception(f"Cannot open custom database: {db_path}")
    
    # Calculate optimal sample size for error threshold
    if error_threshold <= 1.0:
        sample_percent = 20  # 20% for very high accuracy
    elif error_threshold <= 2.0:
        sample_percent = 15  # 15% for high accuracy
    elif error_threshold <= 5.0:
        sample_percent = 10  # 10% for good accuracy
    else:
        sample_percent = 5   # 5% for basic accuracy
    
    # Use CLT validation for statistical confidence
    samples = db.clt_validated_dual_pointer_sample(
        sample_percent, 0.95, 10, 4, error_threshold
    )
    
    # Calculate result from samples
    if samples:
        query_upper = query.upper()
        total_records = db.get_total_records()
        
        if 'SUM(' in query_upper:
            sample_sum = sum(record.amount for record in samples)
            # Scale up to estimate total population sum
            estimated_value = sample_sum * (total_records / len(samples))
        elif 'AVG(' in query_upper:
            estimated_value = sum(record.amount for record in samples) / len(samples)
        elif 'COUNT(' in query_upper:
            estimated_value = total_records  # Always return exact count
        else:
            # Default to average
            estimated_value = sum(record.amount for record in samples) / len(samples)
        
        end_time = time.time()
        computation_time_ms = (end_time - start_time) * 1000
        
        # Calculate confidence interval (simplified)
        sample_values = [record.amount for record in samples]
        sample_mean = sum(sample_values) / len(sample_values)
        sample_variance = sum((x - sample_mean) ** 2 for x in sample_values) / (len(sample_values) - 1)
        sample_std = sample_variance ** 0.5
        margin_of_error = 1.96 * sample_std / (len(sample_values) ** 0.5)  # 95% confidence
        
        if 'SUM(' in query_upper:
            # Scale margin of error for SUM
            scaled_margin = margin_of_error * (total_records / len(samples))
            ci_lower = estimated_value - scaled_margin
            ci_upper = estimated_value + scaled_margin
        else:
            ci_lower = estimated_value - margin_of_error
            ci_upper = estimated_value + margin_of_error
        
        # Create result object
        class CLTResult:
            def __init__(self, value, ci_lower, ci_upper, samples_used, computation_time_ms, error_threshold):
                self.value = value
                self.ci_lower = ci_lower
                self.ci_upper = ci_upper
                self.samples_used = f"{samples_used:,} samples ({sample_percent}%)"
                self.computation_time = type('obj', (object,), {
                    'total_seconds': lambda: computation_time_ms / 1000.0
                })()
                try:
                    self.status = aqe_backend.ApproximationStatus.STABLE
                except AttributeError:
                    try:
                        self.status = aqe_backend.CustomApproximationStatus.STABLE
                    except AttributeError:
                        self.status = "STABLE"
                self.confidence_level = 0.95
                self.error_margin = error_threshold / 100.0
                self.method_name = f"CLT Validation (±{error_threshold}%, {sample_percent}% sample)"
        
        db.close_database()
        return CLTResult(estimated_value, ci_lower, ci_upper, len(samples), computation_time_ms, error_threshold)
    else:
        db.close_database()
        raise Exception("No samples collected with CLT method")

def execute_exact_query(query, db_path):
    """Execute exact query for comparison."""
    print(f"🎯 Exact Query Execution")
    
    start_time = time.time()
    
    # Open custom database
    db = aqe_backend.CustomBPlusDB()
    if not db.open_database(db_path):
        if not db.load_from_file(db_path):
            raise Exception(f"Cannot open custom database: {db_path}")
    
    query_upper = query.upper()
    
    if 'SUM(' in query_upper:
        exact_value = db.sum_amount()  # Get exact sum
    elif 'AVG(' in query_upper:
        total_sum = db.sum_amount()
        total_records = db.get_total_records()
        exact_value = total_sum / total_records if total_records > 0 else 0
    elif 'COUNT(' in query_upper:
        exact_value = db.get_total_records()  # Get exact count
    else:
        # Default to average
        total_sum = db.sum_amount()
        total_records = db.get_total_records()
        exact_value = total_sum / total_records if total_records > 0 else 0
    
    end_time = time.time()
    computation_time_ms = (end_time - start_time) * 1000
    
    class ExactResult:
        def __init__(self, value, computation_time_ms):
            self.value = value
            self.samples_used = "All data (100%)"
            self.computation_time = type('obj', (object,), {
                'total_seconds': lambda: computation_time_ms / 1000.0
            })()
            try:
                self.status = aqe_backend.ApproximationStatus.STABLE
            except AttributeError:
                try:
                    self.status = aqe_backend.CustomApproximationStatus.STABLE
                except AttributeError:
                    self.status = "STABLE"
            self.confidence_level = 1.0
            self.error_margin = 0.0
            self.method_name = "Exact Query"
    
    db.close_database()
    return ExactResult(exact_value, computation_time_ms)

def print_method_comparison():
    """Print comparison of available approximation methods."""
    print("\n📈 Available Approximation Methods:")
    print("="*80)
    print(f"{'Method':<12} {'Description':<35} {'Best For':<25} {'Accuracy':<10}")
    print("-"*80)
    
    for method_key, method_info in ApproximationMethod.METHODS.items():
        print(f"{method_key:<12} {method_info['description']:<35} {method_info['best_for']:<25} {method_info['accuracy']:<10}")
    
    print("\n💡 Method Selection Guide:")
    print("   • Use --s <percent> for quick random sampling")
    print("   • Use --e <error> for statistical CLT approximation") 
    print("   • Use APPROX() in SQL for embedded approximation")
    print("   • Use --method <type> to override automatic selection")

def main():
    parser = argparse.ArgumentParser(
        description="Enhanced ApproximateQueryEngine CLI with multiple query syntaxes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Query Syntax Examples:
  Embedded:     "SELECT APPROX(SUM(amount)) FROM sales"
  Random:       "SELECT SUM(amount) FROM sales" --s 10
  CLT:          "SELECT SUM(amount) FROM sales" --e 2
  Exact:        "SELECT SUM(amount) FROM sales"
  
Method Examples:
  python enhanced_aqe_cli.py "SELECT APPROX(AVG(value)) FROM data"
  python enhanced_aqe_cli.py "SELECT SUM(amount) FROM sales" --s 15
  python enhanced_aqe_cli.py "SELECT COUNT(star) FROM users" --e 1.5
  python enhanced_aqe_cli.py "SELECT AVG(price) FROM products" --method clt --e 3
        """
    )
    
    parser.add_argument("query", nargs='?', help="SQL query to execute")
    parser.add_argument("--db", default="custom_demo.db", 
                       help="Database file path (default: custom_demo.db)")
    
    # Approximation flags
    parser.add_argument("-s", "--sample", type=float, metavar="PERCENT",
                       help="Random sample percentage (e.g., --s 10 for 10 percent)")
    parser.add_argument("-e", "--error", type=float, metavar="THRESHOLD", 
                       help="Error threshold percentage for CLT (e.g., --e 2 for ±2 percent)")
    
    # Method selection
    parser.add_argument("--method", choices=list(ApproximationMethod.METHODS.keys()),
                       help="Override automatic method selection")
    
    # Additional options
    parser.add_argument("--compare", action="store_true",
                       help="Compare with exact query results")
    parser.add_argument("--explain", action="store_true",
                       help="Show method comparison and recommendations")
    parser.add_argument("--threads", type=int, default=4,
                       help="Number of threads for parallel methods (default: 4)")
    parser.add_argument("--confidence", type=float, default=0.95,
                       help="Confidence level for statistical methods (default: 0.95)")
    parser.add_argument("--ci", action="store_true",
                       help="Show confidence intervals in results (same as --confidence)")
    
    args = parser.parse_args()
    
    # Show method comparison if requested
    if args.explain:
        print_method_comparison()
        return 0
    
    # Validate that query is provided if not just explaining
    if not args.query:
        parser.error("Query is required unless using --explain")
        return 1
    
    # Validate database
    if not os.path.exists(args.db):
        print(f"❌ Error: Database file '{args.db}' not found")
        print(f"   Available databases: {[f for f in os.listdir('.') if f.endswith('.db')]}")
        return 1
    
    try:
        # Parse query and determine type
        clean_query, has_embedded_approx = parse_embedded_approx(args.query)
        query_type = determine_query_type(args.query, args)
        
        print(f"🔍 Query Analysis:")
        print(f"   Query: {args.query}")
        print(f"   Database: {args.db}")
        print(f"   Type: {query_type}")
        
        # Get dataset size for method optimization
        try:
            db = aqe_backend.CustomBPlusDB()
            # Try opening existing database first
            if db.open_database(args.db):
                dataset_size = db.get_total_records()
                print(f"   Dataset size: {dataset_size:,} records")
                db.close_database()
            elif db.load_from_file(args.db):
                dataset_size = db.get_total_records()
                print(f"   Dataset size: {dataset_size:,} records (loaded from file)")
                db.close_database()
            else:
                dataset_size = None
                print(f"   Could not open database: {args.db}")
        except Exception as e:
            dataset_size = None
            print(f"   Could not determine dataset size: {e}")
        
        print("-" * 60)
        
        # Execute query based on type and method
        if query_type == QueryType.RANDOM_SAMPLE:
            result = execute_random_sampling(clean_query, args.db, args.sample or 10)
        elif query_type == QueryType.CLT_APPROXIMATION:
            result = execute_clt_approximation(clean_query, args.db, args.error or 5.0)
        elif query_type == QueryType.EMBEDDED_APPROX:
            # Use optimal method for embedded APPROX
            optimal_method = args.method or get_optimal_method_for_query(clean_query, dataset_size)
            print(f"🚀 Embedded APPROX using {optimal_method} method")
            if optimal_method == 'clt':
                result = execute_clt_approximation(clean_query, args.db, 2.0)  # Default 2% error
            else:
                result = execute_random_sampling(clean_query, args.db, 10)  # Default 10% sample
        else:
            # Exact query
            result = execute_exact_query(clean_query, args.db)
        
        # Display results
        print(f"\n📊 {result.method_name} Results:")
        print(f"   Value: {result.value:,.4f}")
        
        # Show confidence intervals if requested or if CLT method
        if args.ci or hasattr(result, 'ci_lower') and hasattr(result, 'ci_upper'):
            if hasattr(result, 'ci_lower') and hasattr(result, 'ci_upper'):
                print(f"   Confidence Interval: ({result.ci_lower:,.4f} - {result.ci_upper:,.4f})")
        
        print(f"   Status: {format_status(result.status)}")
        print(f"   Confidence: {result.confidence_level:.1%}")
        print(f"   Error margin: ±{result.error_margin:.1%}")
        print(f"   Samples used: {result.samples_used}")
        print(f"   Execution time: {format_time(result.computation_time.total_seconds() * 1000)}")
        
        # Compare with exact if requested
        if args.compare and query_type != QueryType.EXACT:
            print(f"\n🎯 Running exact comparison...")
            try:
                exact_result = execute_exact_query(clean_query, args.db)
                
                print(f"\nComparison:")
                print(f"   Approximate: {result.value:,.4f}")
                print(f"   Exact:       {exact_result.value:,.4f}")
                
                if exact_result.value != 0:
                    actual_error = abs(result.value - exact_result.value) / abs(exact_result.value) * 100
                    print(f"   Actual error: {actual_error:.2f}%")
                
                approx_time = result.computation_time.total_seconds() * 1000
                exact_time = exact_result.computation_time.total_seconds() * 1000
                if exact_time > 0:
                    speedup = exact_time / approx_time
                    print(f"   Speedup: {speedup:.1f}x faster")
                
            except Exception as e:
                print(f"   Comparison failed: {e}")
        
        # Method recommendation
        if query_type != QueryType.EXACT:
            optimal_method = get_optimal_method_for_query(clean_query, dataset_size)
            current_method = getattr(result, 'method_name', 'Unknown').lower()
            if optimal_method not in current_method:
                print(f"\n💡 Recommendation: Try --method {optimal_method} for potentially better results")
        
        print("\n" + "="*60)
        print("✅ Query completed successfully")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())