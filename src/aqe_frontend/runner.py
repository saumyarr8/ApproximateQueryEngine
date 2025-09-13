"""
Runner: connects parsed queries to backend methods.
"""

import sys
import os

# Add the project root to Python path to find aqe_backend
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(project_root, 'src'))

try:
    # Import the compiled C++ module from aqe_backend package
    from aqe_backend import aqe_backend
    backend_module = aqe_backend
except ImportError:
    try:
        # Fallback: try importing from build directory
        build_path = os.path.join(project_root, 'build', 'src', 'aqe_backend', 'bindings')
        sys.path.insert(0, build_path)
        import aqe_backend as backend_module
    except ImportError:
        backend_module = None

from .parser import parse_query


def run_query(query: str, db_path: str = "example.db"):
    if backend_module is None:
        raise ImportError("aqe_backend extension not found. Build the C++ backend first.")

    parsed = parse_query(query)
    db = backend_module.DB(db_path)

    if parsed["agg_func"] == "SUM":
        return db.execute_sum(parsed["table"], parsed["column"])
    elif parsed["agg_func"] == "COUNT":
        return db.execute_count(parsed["table"], parsed["column"])
    elif parsed["agg_func"] == "AVG":
        return db.execute_avg(parsed["table"], parsed["column"])
    else:
        raise ValueError(f"Unsupported aggregation: {parsed['agg_func']}")
