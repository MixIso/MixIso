# MixIso Experimental Manual (Current Version)

This repository contains the experimental code implementation for the paper *On Mixing Database Isolation Levels*. The current version is primarily **Java CLI**-based, supporting:

- Benchmark workload generation
- Isolation level allocation
- Distributed execution simulation (default: 100-300ms latency)
- Performance comparison of three execution strategies (SER / SI-SER / PC-SI-SER)

## 1. System Requirements

- Java 17+
- Maven 3.8+

> Note: Windows PowerShell uses `;` as the classpath separator; Linux/macOS use `:`.

## 2. Build & Dependencies

Execute in the project root directory:

```sh
mvn -DskipTests compile
mvn dependency:copy-dependencies
```

Explanation:

- `compile` generates `target/classes`
- `copy-dependencies` generates `target/dependency/*` for running `java -cp` commands

## 3. Project Directory (Experiment Related)

```plain
MixIso
├─ src/main/java/algorithm/
│  ├─ Allocator.java
│  ├─ BenchWorkloadGenerator.java
│  ├─ BenchWorkloadAllocatorBatch.java
│  ├─ BenchWorkloadExecutorBatch.java
│  ├─ BenchStrategyComparisonBatch.java
│  ├─ RandomWorkloadGenerator.java
│  ├─ RandomWorkloadAllocatorBatch.java
│  ├─ RandomWorkloadExperiment.java
│  └─ VisualizationExporter.java
└─ data/
    ├─ bench_workload/
    ├─ allocated_bench_workload/
    ├─ benchmarks/
    └─ *.csv / *.png
```

## 4. Core Commands

### 4.1 Single File Allocation / Evaluation

```sh
# Evaluate (benchmark mode)
java -cp "target/classes;target/dependency/*" algorithm.Allocator benchmark <workload_file> <output_csv> [warmups] [iterations]

# Allocate (allocate mode)
java -cp "target/classes;target/dependency/*" algorithm.Allocator allocate <input_workload> <output_workload>
```

### 4.2 Batch Processing Workflow (Recommended)

```sh
# 1) Generate benchmark workload (10 sessions, 20 txns per session, 50k keys, 1 case)
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadGenerator --sessions 10 --txns-per-session 20 --max-key 50000 --cases 1

# 2) Batch allocation of isolation levels (exports distribution charts)
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadAllocatorBatch

# 3) Batch execution of allocated workloads (default: 5 DCs, 100-300ms latency)
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadExecutorBatch
```

`BenchWorkloadExecutorBatch` parameters:

```plain
java ... algorithm.BenchWorkloadExecutorBatch [dcCount] [minRttMs] [maxRttMs] [outputCsv]
```

Current defaults:

- `dcCount=5`
- `minRttMs=100`
- `maxRttMs=300`
- `outputCsv=data/bench_execution_results.csv`

Example (explicit parameters):

```sh
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadExecutorBatch 5 100 300 data/bench_execution_results.csv
```

## 5. Three-Strategy Comparison Experiment (Q3 Focus)

Use the following command to complete the three-strategy comparison in one go:

```sh
java -cp "target/classes;target/dependency/*" algorithm.BenchStrategyComparisonBatch
```

Parameter format:

```plain
java ... algorithm.BenchStrategyComparisonBatch [dcCount] [minRttMs] [maxRttMs] [detailCsv] [summaryCsv] [outputPng]
```

Current defaults:

- `dcCount=5`
- `minRttMs=100`
- `maxRttMs=300`
- `detailCsv=data/bench_strategy_comparison_details.csv`
- `summaryCsv=data/bench_strategy_comparison_summary.csv`
- `outputPng=data/bench_strategy_comparison.png`

Strategy mapping (fixed in code):

- `SER` ← `ExecutionStrategy.ALL_SER`
- `SI-SER` ← `ExecutionStrategy.NON_SER_AS_SI`
- `PC-SI-SER` ← `ExecutionStrategy.CURRENT`

Output files:

- `data/bench_strategy_comparison_details.csv`: Details for each workload × each strategy
- `data/bench_strategy_comparison_summary.csv`: Mean values aggregated by benchmark
- `data/bench_strategy_comparison.png`: Throughput/latency comparison chart

## 6. Random Workload Experiments (Q2)

```sh
# 1) Generate random workload
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadGenerator --txns 500 --max-ops 10 --max-key 500000 --read-only 30 --cases 5

# 2) Batch allocation and performance statistics
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadAllocatorBatch

# 3) Controlled variable experiment
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadExperiment
```

Common outputs:

- `data/allocation_performance.csv`
- `data/allocation_performance_analysis.csv`
- `data/allocation_performance.png`
- `data/allocation_experiment_summary.csv`

## 7. One-Shot Experiment Reproduction (Recommended Order)

```sh
# Build
mvn -DskipTests compile
mvn dependency:copy-dependencies

# Q1: Allocation effectiveness
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadGenerator --sessions 10 --txns-per-session 20 --max-key 50 --cases 5
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadAllocatorBatch

# Q3: Execution performance
java -cp "target/classes;target/dependency/*" algorithm.BenchWorkloadExecutorBatch 5 100 300

# Q3: Three-strategy cross-comparison
java -cp "target/classes;target/dependency/*" algorithm.BenchStrategyComparisonBatch 5 100 300

# Q2: Random workload efficiency
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadGenerator --txns 500 --max-ops 10 --max-key 500000 --read-only 30 --cases 5
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadAllocatorBatch
java -cp "target/classes;target/dependency/*" algorithm.RandomWorkloadExperiment
```

## 8. FAQ

1. **`ClassNotFoundException: com.fasterxml.jackson...`**
    - First run: `mvn dependency:copy-dependencies`

2. **PowerShell command not found (e.g., `head`)**
    - Use `Select-Object -First N` instead

3. **To further accelerate experiments**
    - Maintain the default 100-300ms latency, or manually pass a smaller range (e.g., `50 150`)
