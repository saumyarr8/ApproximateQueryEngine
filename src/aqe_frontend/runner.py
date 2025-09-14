"""
Runner: connects parsed queries to backend methods.
"""

import sys
import os

# Add the build directory to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
build_path = os.path.join(project_root, 'build', 'src', 'aqe_backend')
sys.path.insert(0, build_path)

try:
    import aqe_backend
except ImportError:
    raise ImportError("aqe_backend extension not found. Build the C++ backend first.")

from .parser import parse_query

def run_query(query: str, db_path: str, sample_percent: int = 0) -> float:
    """Run a single aggregate query with optional sampling"""
    return aqe_backend.run_query(query, db_path, sample_percent)

def run_query_groupby(query: str, db_path: str, sample_percent: int = 0, num_threads: int = 4) -> dict:
    """Run a GROUP BY query with optional sampling"""
    return aqe_backend.run_query_groupby(query, db_path, sample_percent, num_threads)

def run_query_with_ci(query: str, db_path: str = "example.db", sample_percent: int = 0):
    """
    Run a query with confidence intervals for the approximate result.
    
    Args:
        query: SQL query string
        db_path: Path to the database file
        sample_percent: Percentage to sample (0 = exact query)
        
    Returns:
        QueryResult object with value, ci_lower, and ci_upper properties
    """
    if backend_module is None:
        raise ImportError("aqe_backend extension not found. Build the C++ backend first.")
    
    return backend_module.run_query_with_ci(query, db_path, sample_percent)

def run_query_groupby_with_ci(query: str, db_path: str = "example.db", sample_percent: int = 0, num_threads: int = 4):
    """
    Run a GROUP BY query with confidence intervals for each group.
    
    Args:
        query: SQL query string with GROUP BY clause
        db_path: Path to the database file
        sample_percent: Percentage to sample (0 = exact query)
        num_threads: Number of threads for parallel execution
        
    Returns:
        Dictionary mapping group values to QueryResult objects
    """
    if backend_module is None:
        raise ImportError("aqe_backend extension not found. Build the C++ backend first.")
    
    return backend_module.run_query_groupby_with_ci(query, db_path, sample_percent, num_threads)