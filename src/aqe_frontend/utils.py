"""
Helper utilities for the frontend.
"""

import sqlite3
import random
import os
import time
import csv
import json
import matplotlib.pyplot as plt
import numpy as np
from typing import List, Dict, Any, Optional, Tuple

def create_example_db(db_name="example.db"):
    """Create a simple example database with a single table."""
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS mytable;")
    cur.execute("CREATE TABLE mytable (id INTEGER PRIMARY KEY, value REAL);")

    values = [(i, float(i) * 1.5) for i in range(1, 11)]
    cur.executemany("INSERT INTO mytable (id, value) VALUES (?, ?);", values)

    conn.commit()
    conn.close()
    print(f"Database '{db_name}' created with sample data.")

def create_sales_db(db_name="sales.db", rows=100000):
    """Create a larger sales database for benchmarking."""
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS sales")
    cur.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY AUTOINCREMENT, amount REAL, category TEXT)")
    
    categories = ["A", "B", "C", "D"]
    batch_size = 10000
    
    for i in range(0, rows, batch_size):
        chunk = min(batch_size, rows - i)
        data = [(random.uniform(1, 1000), random.choice(categories)) for _ in range(chunk)]
        cur.executemany("INSERT INTO sales (amount, category) VALUES (?, ?)", data)
        conn.commit()
        print(f"Inserted {i+chunk}/{rows} rows")
    
    conn.close()
    print(f"Database '{db_name}' created with {rows} rows of sales data.")

def run_timed_query(query_func, *args, **kwargs):
    """Run a query and measure its execution time."""
    start_time = time.time()
    result = query_func(*args, **kwargs)
    elapsed = time.time() - start_time
    return result, elapsed

def benchmark_query(query: str, db_path: str, 
                   sample_rates: List[int] = [0, 1, 5, 10, 20, 50], 
                   runs: int = 3) -> List[Dict[str, Any]]:
    """
    Benchmark a query at different sample rates.
    
    Args:
        query: SQL query to execute
        db_path: Path to the database
        sample_rates: List of sample percentages to test
        runs: Number of times to run each query
    
    Returns:
        List of result dictionaries with metrics
    """
    from .runner import run_query
    
    results = []
    
    # First run exact query to get baseline
    exact_result = None
    exact_time = 0
    
    for sample in sample_rates:
        print(f"Testing sample rate {sample}%...")
        sample_times = []
        sample_results = []
        
        for run in range(runs):
            result, elapsed = run_timed_query(run_query, query, db_path, sample)
            
            sample_times.append(elapsed)
            sample_results.append(result)
            
            if sample == 0 and run == 0:
                exact_result = result
                exact_time = elapsed
        
        avg_time = sum(sample_times) / len(sample_times)
        avg_result = sum(sample_results) / len(sample_results)
        
        # Calculate error and speedup
        if exact_result is not None and sample > 0:
            error = abs(avg_result - exact_result) / abs(exact_result) if exact_result != 0 else float('inf')
            speedup = exact_time / avg_time
        else:
            error = 0
            speedup = 1
        
        results.append({
            'sample_rate': sample,
            'avg_result': avg_result,
            'avg_time': avg_time,
            'min_time': min(sample_times),
            'max_time': max(sample_times),
            'error': error,
            'speedup': speedup
        })
    
    return results

def benchmark_adaptive_query(query: str, db_path: str, runs: int = 3) -> Dict[str, Any]:
    """
    Benchmark adaptive sampling vs traditional sampling.
    
    Args:
        query: SQL query to execute
        db_path: Path to the database
        runs: Number of times to run each approach
    
    Returns:
        Dictionary with comparison metrics
    """
    from .runner import run_query
    
    # Import adaptive sampler
    import sys
    import os
    build_path = os.path.join(os.path.dirname(__file__), '..', '..', 'build', 'src', 'aqe_backend')
    if build_path not in sys.path:
        sys.path.insert(0, build_path)
    
    try:
        import aqe_backend
        
        # Test traditional sampling
        traditional_results = []
        for sample_rate in [5, 10, 20]:
            times = []
            results = []
            for _ in range(runs):
                start = time.time()
                result = run_query(query, db_path, sample_rate)
                elapsed = time.time() - start
                times.append(elapsed)
                results.append(result)
            
            traditional_results.append({
                'sample_rate': sample_rate,
                'avg_time': np.mean(times),
                'avg_result': np.mean(results),
                'confidence': 0.8  # Static estimate
            })
        
        # Test adaptive sampling
        adaptive_results = []
        sampler = aqe_backend.AdaptiveSampler(db_path, 0.05)
        
        for _ in range(runs):
            result = sampler.execute_adaptive_query(query, 10, 0.90)
            adaptive_results.append({
                'time': result.computation_time.total_seconds(),
                'result': result.value,
                'confidence': result.confidence_level,
                'status': str(result.status),
                'samples_used': result.samples_used
            })
        
        return {
            'traditional': traditional_results,
            'adaptive': adaptive_results,
            'adaptive_avg_time': np.mean([r['time'] for r in adaptive_results]),
            'adaptive_avg_confidence': np.mean([r['confidence'] for r in adaptive_results]),
            'adaptive_stable_rate': sum(1 for r in adaptive_results if 'STABLE' in r['status']) / len(adaptive_results)
        }
        
    except ImportError as e:
        print(f"Adaptive sampling not available: {e}")
        return {'error': 'Adaptive sampling module not found'}

def plot_benchmark_results(results: List[Dict[str, Any]], 
                          title: str = "Query Performance", 
                          output_file: Optional[str] = None):
    """
    Plot benchmark results showing error vs speedup tradeoff.
    
    Args:
        results: List of benchmark result dictionaries
        title: Plot title
        output_file: Path to save the plot (None = display only)
    """
    # Extract data for plotting
    sample_rates = [r['sample_rate'] for r in results if r['sample_rate'] > 0]
    errors = [r['error'] * 100 for r in results if r['sample_rate'] > 0]
    speedups = [r['speedup'] for r in results if r['sample_rate'] > 0]
    times = [r['avg_time'] for r in results if r['sample_rate'] > 0]
    
    # Create figure with subplots
    fig, axs = plt.subplots(2, 2, figsize=(12, 10))
    
    # Plot 1: Error vs Sample Rate
    axs[0, 0].plot(sample_rates, errors, 'o-')
    axs[0, 0].set_xlabel('Sample Rate (%)')
    axs[0, 0].set_ylabel('Relative Error (%)')
    axs[0, 0].set_title('Error vs Sample Rate')
    axs[0, 0].grid(True)
    
    # Plot 2: Speedup vs Sample Rate
    axs[0, 1].plot(sample_rates, speedups, 'o-')
    axs[0, 1].set_xlabel('Sample Rate (%)')
    axs[0, 1].set_ylabel('Speedup Factor')
    axs[0, 1].set_title('Speedup vs Sample Rate')
    axs[0, 1].grid(True)
    
    # Plot 3: Error vs Speedup (tradeoff)
    axs[1, 0].plot(speedups, errors, 'o-')
    axs[1, 0].set_xlabel('Speedup Factor')
    axs[1, 0].set_ylabel('Relative Error (%)')
    axs[1, 0].set_title('Error vs Speedup (Tradeoff)')
    axs[1, 0].grid(True)
    
    # Plot 4: Execution Time vs Sample Rate
    axs[1, 1].plot(sample_rates, times, 'o-')
    axs[1, 1].set_xlabel('Sample Rate (%)')
    axs[1, 1].set_ylabel('Execution Time (s)')
    axs[1, 1].set_title('Time vs Sample Rate')
    axs[1, 1].grid(True)
    
    plt.suptitle(title)
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file)
        print(f"Plot saved to {output_file}")
    
    plt.show()

def save_benchmark_results(results: List[Dict[str, Any]], 
                          output_file: str = "benchmark_results.csv"):
    """
    Save benchmark results to a CSV file.
    
    Args:
        results: List of benchmark result dictionaries
        output_file: Path to save the CSV file
    """
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['sample_rate', 'avg_result', 'avg_time', 'min_time', 
                     'max_time', 'error', 'speedup']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in results:
            writer.writerow(row)
    
    print(f"Results saved to {output_file}")

def demo_adaptive_vs_traditional(db_path: str = "sales.db"):
    """
    Demo function comparing adaptive vs traditional sampling.
    """
    print("ADAPTIVE vs TRADITIONAL SAMPLING DEMO")
    print("=" * 50)
    
    query = "SELECT AVG(amount) FROM sales"
    
    # Traditional sampling
    print("\nTraditional Fixed Sampling:")
    from .runner import run_query
    
    for rate in [5, 10, 20]:
        start = time.time()
        result = run_query(query, db_path, rate)
        elapsed = time.time() - start
        print(f"  {rate}% sample: {result:.2f} in {elapsed:.3f}s")
    
    # Adaptive sampling
    print("\nAdaptive Fast-Slow Pointer Sampling:")
    try:
        import sys
        import os
        build_path = os.path.join(os.path.dirname(__file__), '..', '..', 'build', 'src', 'aqe_backend')
        if build_path not in sys.path:
            sys.path.insert(0, build_path)
        
        import aqe_backend
        
        sampler = aqe_backend.AdaptiveSampler(db_path, 0.05)
        result = sampler.execute_adaptive_query(query, 10, 0.90)
        
        print(f"  Result: {result.value:.2f}")
        print(f"  Status: {result.status}")
        print(f"  Confidence: {result.confidence_level:.1%}")
        print(f"  Time: {result.computation_time.total_seconds():.3f}s")
        print(f"  Validation samples: {result.samples_used}")
        
        if result.status == aqe_backend.ApproximationStatus.STABLE:
            print("  Approximation is statistically stable!")
        elif result.status == aqe_backend.ApproximationStatus.DRIFTING:
            print("  Approximation may be drifting - consider larger sample")
        else:
            print("  Insufficient data for validation")
            
    except ImportError:
        print("  Adaptive sampling module not available")
    except Exception as e:
        print(f"  Error: {e}")
    
    print("\nAdaptive sampling provides automatic quality validation!")

import sqlite3
import random
import os
import time
import csv
import json
import matplotlib.pyplot as plt
import numpy as np
from typing import List, Dict, Any, Optional, Tuple

def create_example_db(db_name="example.db"):
    """Create a simple example database with a single table."""
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS mytable;")
    cur.execute("CREATE TABLE mytable (id INTEGER PRIMARY KEY, value REAL);")

    values = [(i, float(i) * 1.5) for i in range(1, 11)]
    cur.executemany("INSERT INTO mytable (id, value) VALUES (?, ?);", values)

    conn.commit()
    conn.close()
    print(f"Database '{db_name}' created with sample data.")

def create_sales_db(db_name="sales.db", rows=100000):
    """Create a larger sales database for benchmarking."""
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS sales")
    cur.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY AUTOINCREMENT, amount REAL, category TEXT)")
    
    categories = ["A", "B", "C", "D"]
    batch_size = 10000
    
    for i in range(0, rows, batch_size):
        chunk = min(batch_size, rows - i)
        data = [(random.uniform(1, 1000), random.choice(categories)) for _ in range(chunk)]
        cur.executemany("INSERT INTO sales (amount, category) VALUES (?, ?)", data)
        conn.commit()
        print(f"Inserted {i+chunk}/{rows} rows")
    
    conn.close()
    print(f"Database '{db_name}' created with {rows} rows of sales data.")

def run_timed_query(query_func, *args, **kwargs):
    """Run a query and measure its execution time."""
    start_time = time.time()
    result = query_func(*args, **kwargs)
    elapsed = time.time() - start_time
    return result, elapsed

def benchmark_query(query: str, db_path: str, 
                   sample_rates: List[int] = [0, 1, 5, 10, 20, 50], 
                   runs: int = 3) -> List[Dict[str, Any]]:
    """
    Benchmark a query at different sample rates.
    
    Args:
        query: SQL query to execute
        db_path: Path to the database
        sample_rates: List of sample percentages to test
        runs: Number of times to run each query
    
    Returns:
        List of result dictionaries with metrics
    """
    from .runner import run_query
    
    results = []
    
    # First run exact query to get baseline
    exact_result = None
    exact_time = 0
    
    for sample in sample_rates:
        print(f"Testing sample rate {sample}%...")
        sample_times = []
        sample_results = []
        
        for run in range(runs):
            result, elapsed = run_timed_query(run_query, query, db_path, sample)
            
            sample_times.append(elapsed)
            sample_results.append(result)
            
            if sample == 0 and run == 0:
                exact_result = result
                exact_time = elapsed
        
        avg_time = sum(sample_times) / len(sample_times)
        avg_result = sum(sample_results) / len(sample_results)
        
        # Calculate error and speedup
        if exact_result is not None and sample > 0:
            error = abs(avg_result - exact_result) / abs(exact_result) if exact_result != 0 else float('inf')
            speedup = exact_time / avg_time
        else:
            error = 0
            speedup = 1
        
        results.append({
            'sample_rate': sample,
            'avg_result': avg_result,
            'avg_time': avg_time,
            'min_time': min(sample_times),
            'max_time': max(sample_times),
            'error': error,
            'speedup': speedup
        })
    
    return results

def plot_benchmark_results(results: List[Dict[str, Any]], 
                          title: str = "Query Performance", 
                          output_file: Optional[str] = None):
    """
    Plot benchmark results showing error vs speedup tradeoff.
    
    Args:
        results: List of benchmark result dictionaries
        title: Plot title
        output_file: Path to save the plot (None = display only)
    """
    # Extract data for plotting
    sample_rates = [r['sample_rate'] for r in results if r['sample_rate'] > 0]
    errors = [r['error'] for r in results if r['sample_rate'] > 0]
    speedups = [r['speedup'] for r in results if r['sample_rate'] > 0]
    times = [r['avg_time'] for r in results if r['sample_rate'] > 0]
    
    # Create figure with subplots
    fig, axs = plt.subplots(2, 2, figsize=(12, 10))
    
    # Plot 1: Error vs Sample Rate
    axs[0, 0].plot(sample_rates, errors, 'o-')
    axs[0, 0].set_xlabel('Sample Rate (%)')
    axs[0, 0].set_ylabel('Relative Error')
    axs[0, 0].set_title('Error vs Sample Rate')
    axs[0, 0].grid(True)
    
    # Plot 2: Speedup vs Sample Rate
    axs[0, 1].plot(sample_rates, speedups, 'o-')
    axs[0, 1].set_xlabel('Sample Rate (%)')
    axs[0, 1].set_ylabel('Speedup Factor')
    axs[0, 1].set_title('Speedup vs Sample Rate')
    axs[0, 1].grid(True)
    
    # Plot 3: Error vs Speedup (tradeoff)
    axs[1, 0].plot(speedups, errors, 'o-')
    axs[1, 0].set_xlabel('Speedup Factor')
    axs[1, 0].set_ylabel('Relative Error')
    axs[1, 0].set_title('Error vs Speedup (Tradeoff)')
    axs[1, 0].grid(True)
    
    # Plot 4: Execution Time vs Sample Rate
    axs[1, 1].plot(sample_rates, times, 'o-')
    axs[1, 1].set_xlabel('Sample Rate (%)')
    axs[1, 1].set_ylabel('Execution Time (s)')
    axs[1, 1].set_title('Time vs Sample Rate')
    axs[1, 1].grid(True)
    
    plt.suptitle(title)
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file)
        print(f"Plot saved to {output_file}")
    
    plt.show()

def save_benchmark_results(results: List[Dict[str, Any]], 
                          output_file: str = "benchmark_results.csv"):
    """
    Save benchmark results to a CSV file.
    
    Args:
        results: List of benchmark result dictionaries
        output_file: Path to save the CSV file
    """
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['sample_rate', 'avg_result', 'avg_time', 'min_time', 
                     'max_time', 'error', 'speedup']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in results:
            writer.writerow(row)
    
    print(f"Results saved to {output_file}")