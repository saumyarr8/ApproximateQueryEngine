# Approximate Query Engine

High-performance database query acceleration using statistical sampling and multithreading.

## Performance
- **Up to 234× speedup** over traditional B-tree queries
- **95.8% accuracy** with 155× speedup on 10M records
- **Statistical confidence intervals** with CLT validation

## System Architecture

### Overview
```
┌─────────────────────────────────────────────────────────────────┐
│                    USER INTERFACE LAYER                        │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │  Enhanced CLI   │  │ Query Parser    │  │ Result Handler  │ │
│  │ (3 Syntaxes)    │  │                 │  │                 │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────┬───────────────────────────────┬───────────────┘
                  │                               │
                  ▼                               ▼
          ┌───────────────┐               ┌───────────────┐
          │   Python      │               │   Result      │
          │  Frontend     │               │ Formatting    │
          └───────┬───────┘               └───────────────┘
                  │
                  ▼
          ┌───────────────┐
          │   pybind11    │
          │    Bridge     │
          └───────┬───────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                   C++ BACKEND LAYER                            │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │ Custom B+ Tree  │──│ Memory Mapping  │──│ Multithreading  │ │
│  │   Database      │  │    Engine       │  │   Scheduler     │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────┬───────────────────────────────┬───────────────┘
                  │                               │
                  ▼                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                  STATISTICAL LAYER                             │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │      CLT        │  │  Fast/Slow      │  │ Signal-Based    │ │
│  │  Validation     │  │   Pointers      │  │ Coordination    │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Query Processing Flow
```
User Input
    │
    ▼
┌───────────────┐    SQL Query Processing
│ Query Parser  │ ◄──── "SELECT SUM(amount) FROM sales"
└───────┬───────┘
        │
        ▼
┌───────────────┐    Query Type Detection
│ Type Detector │ ◄──── APPROX(), --s, --e flags
└───────┬───────┘
        │
        ▼
┌───────────────┐    Method Selection
│ Method Router │ ◄──── random, clt, parallel, block
└───────┬───────┘
        │
        ▼
┌───────────────┐    Execution Path
│ Executor      │ ──┬─► Random Sampling (--s)
└───────────────┘   ├─► CLT Statistical (--e)
                    └─► Embedded APPROX()
        │
        ▼
┌───────────────┐    C++ Backend
│ pybind11      │ ──► CustomBPlusDB
└───────┬───────┘     Memory Mapping
        │              Multithreading
        ▼
┌───────────────┐    Results
│ Statistical   │ ──► Confidence Intervals
│ Processing    │     Error Margins
└───────┬───────┘     Sample Counts
        │
        ▼
┌───────────────┐    Output
│ Result        │ ──► Formatted Results
│ Formatter     │     Performance Metrics
└───────────────┘
```

### Threading Architecture
```
Main Thread (CLI Validator)
    │
    ├─── Query Processing
    │    ├─── Parse SQL
    │    ├─── Detect Type
    │    └─── Route Method
    │
    ▼
┌───────────────────────────────────┐
│        Signal Validator Thread    │  ◄─── Coordination Hub
│                                   │
│  ┌─────────────────────────────┐  │
│  │ CLT Convergence Monitoring  │  │
│  │ Error Threshold Validation  │  │
│  │ Early Termination Signals   │  │
│  └─────────────────────────────┘  │
└─────────────┬─────────────────────┘
              │
              ▼
    Thread Pool Manager
              │
    ┌─────────┴─────────┐
    │                   │
    ▼                   ▼
Fast Pointer Threads   Slow Pointer Threads
    (n/2)                 (n/2)
    │                     │
    ├─► Thread 1: Region 1 ──► Large Steps, Quick Coverage
    ├─► Thread 2: Region 2 ──► Large Steps, Quick Coverage  
    └─► Thread n/2: Region n/2 ──► Large Steps, Quick Coverage
                          │
                          ├─► Thread 1: Region 1 ──► Small Steps, Validation
                          ├─► Thread 2: Region 2 ──► Small Steps, Validation
                          └─► Thread n/2: Region n/2 ──► Small Steps, Validation

Signal Flow:
CLI Validator ──► Signal Validator ──► Fast/Slow Pointers
      │               │                        │
      │               ▼                        │
      │     ┌─────────────────┐                │
      │     │ Atomic Signals: │                │
      │     │ should_stop     │◄───────────────┘
      │     │ sample_count    │
      │     │ current_mean    │
      │     │ convergence     │
      │     └─────────────────┘
      │               │
      └─────────────► Result Aggregation
```

### Performance Optimization Path
```
Input: Large Dataset (10M+ Records)
           │
           ▼
    ┌─────────────┐
    │ Query Type  │ ◄─── Automatic Detection
    │ Analysis    │      SUM, AVG, COUNT
    └──────┬──────┘
           │
    ┌──────▼──────┐
    │ Method      │ ◄─── User Choice or Auto-select
    │ Selection   │      random, clt, parallel
    └──────┬──────┘
           │
    ┌──────▼──────┐
    │ Sample Size │ ◄─── Error Threshold/Percentage
    │ Calculator  │      Statistical Requirements
    └──────┬──────┘
           │
    ┌──────▼──────┐
    │ Memory Map  │ ◄─── O(1) Direct Access
    │ Database    │      Eliminate I/O Bottleneck
    └──────┬──────┘
           │
    ┌──────▼──────┐
    │ Multi-Core  │ ◄─── CPU Parallelization
    │ Sampling    │      Fast/Slow Pointers
    └──────┬──────┘
           │
    ┌──────▼──────┐
    │ Statistical │ ◄─── CLT Validation
    │ Validation  │      Confidence Intervals
    └──────┬──────┘
           │
           ▼
    High-Speed Results: 155× - 234× Speedup
    High Accuracy: 95.8% - 99.96%
```

## Quick Start

### 1. Build
```bash
python setup.py build_ext --inplace
```

### 2. Create Database
```python
from src.aqe_frontend.utils import create_sales_db
create_sales_db("demo.db", rows=100000)
```

### 3. Run Queries

**CLI Usage:**
```bash
# Random sampling (10%)
python enhanced_aqe_cli.py "SELECT SUM(amount) FROM sales" --s 10

# CLT statistical method (±2% error)
python enhanced_aqe_cli.py "SELECT AVG(amount) FROM sales" --e 2

# Embedded APPROX syntax
python enhanced_aqe_cli.py "SELECT APPROX_SUM(amount, 15) FROM sales"

# Show confidence intervals
python enhanced_aqe_cli.py "SELECT SUM(amount) FROM sales" --s 10 --ci
```

**Python Usage:**
```python
import sys
sys.path.insert(0, 'build/src/aqe_backend')
import aqe_backend

# Open database
db = aqe_backend.CustomBPlusDB()
db.open_database("demo.db")

# Fast sampling (5% sample, 4 threads)
samples = db.random_sample(5.0)

# CLT method with error threshold
samples = db.clt_validated_dual_pointer_sample(10.0, 0.95, 10, 4, 2.0)
```

## Methods
- **random**: Random sampling
- **clt**: Central Limit Theorem with statistical validation
- **parallel**: Multi-threaded fast/slow pointers
- **block**: Block-based sampling
- **adaptive**: Dynamic sample size adjustment

## Key Components
- **Frontend**: Python CLI with 3 query syntaxes
- **Backend**: C++ B+ tree with memory mapping  
- **Statistical**: CLT validation, confidence intervals
- **Threading**: Fast/slow pointer algorithms with signal coordination

## Requirements
- Python 3.10+
- C++17 compiler
- CMake
- pybind11