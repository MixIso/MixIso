package algorithm;

import model.execution.ExecutionResult;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchStrategyComparisonBatch {
	private static class Stats {
		double throughputSum = 0.0;
		double avgLatencySum = 0.0;
		int count = 0;

		void add(ExecutionResult result) {
			throughputSum += result.getThroughputTxPerSec();
			avgLatencySum += result.getAvgLatencyMs();
			count++;
		}

		double meanThroughput() {
			return count == 0 ? 0.0 : throughputSum / count;
		}

		double meanLatency() {
			return count == 0 ? 0.0 : avgLatencySum / count;
		}
	}

	public static void main(String[] args) throws Exception {
		int dcCount = args.length >= 1 ? Integer.parseInt(args[0]) : 5;
		int minRtt = args.length >= 2 ? Integer.parseInt(args[1]) : 10;
		int maxRtt = args.length >= 3 ? Integer.parseInt(args[2]) : 30;
		String detailCsv = args.length >= 4 ? args[3] : "data/bench_strategy_comparison_details.csv";
		String summaryCsv = args.length >= 5 ? args[4] : "data/bench_strategy_comparison_summary.csv";
		String outputPng = args.length >= 6 ? args[5] : "data/bench_strategy_comparison.png";

		Path projectDir = Paths.get(".").toAbsolutePath().normalize();
		Path allocatedDir = projectDir.resolve("data").resolve("allocated_bench_workload");
		Path detailPath = projectDir.resolve(detailCsv);
		Path summaryPath = projectDir.resolve(summaryCsv);
		Path pngPath = projectDir.resolve(outputPng);

		if (detailPath.getParent() != null) {
			Files.createDirectories(detailPath.getParent());
		}
		if (summaryPath.getParent() != null) {
			Files.createDirectories(summaryPath.getParent());
		}
		if (pngPath.getParent() != null) {
			Files.createDirectories(pngPath.getParent());
		}

		List<Path> files = new ArrayList<>();
		Files.list(allocatedDir)
				.filter(p -> p.getFileName().toString().endsWith(".json"))
				.sorted(Comparator.comparing(p -> p.getFileName().toString()))
				.forEach(files::add);

		if (files.isEmpty()) {
			System.out.println("No allocated benchmark files found in " + allocatedDir);
			return;
		}

		List<Executor.ExecutionStrategy> strategies = Arrays.asList(
				Executor.ExecutionStrategy.ALL_SER,
				Executor.ExecutionStrategy.NON_SER_AS_SI,
				Executor.ExecutionStrategy.CURRENT
		);

		Map<Executor.ExecutionStrategy, Stats> statsMap = new EnumMap<>(Executor.ExecutionStrategy.class);
		Map<String, Map<Executor.ExecutionStrategy, Stats>> benchmarkStats = new HashMap<>();
		for (Executor.ExecutionStrategy strategy : strategies) {
			statsMap.put(strategy, new Stats());
		}

		int totalRuns = strategies.size() * files.size();
		int runIdx = 0;
		int failures = 0;

		try (PrintWriter detailWriter = new PrintWriter(new FileWriter(detailPath.toFile()))) {
			detailWriter.println("strategy,file,benchmark,committed,aborted,throughput_tx_per_sec,avg_latency_ms");

			for (Executor.ExecutionStrategy strategy : strategies) {
				String strategyName = displayName(strategy);
				for (Path file : files) {
					runIdx++;
					System.out.printf("[RUN] (%d/%d) strategy=%s file=%s%n", runIdx, totalRuns, strategyName, file.getFileName());
					try {
						ExecutionResult result = Executor.runSingleWorkload(file.toString(), null, dcCount, minRtt, maxRtt, strategy);
						detailWriter.printf("%s,%s,%s,%d,%d,%.4f,%.4f%n",
								strategyName,
								result.getFileName(),
								result.getBenchmark(),
								result.getCommittedTxns(),
								result.getAbortedTxns(),
								result.getThroughputTxPerSec(),
								result.getAvgLatencyMs());
						statsMap.get(strategy).add(result);
						benchmarkStats
								.computeIfAbsent(result.getBenchmark(), k -> new EnumMap<>(Executor.ExecutionStrategy.class))
								.computeIfAbsent(strategy, k -> new Stats())
								.add(result);
						System.out.printf("[OK] strategy=%s file=%s throughput=%.2f avgLatency=%.2f%n",
								strategyName,
								file.getFileName(),
								result.getThroughputTxPerSec(),
								result.getAvgLatencyMs());
					} catch (Exception e) {
						failures++;
						System.out.printf("[FAIL] strategy=%s file=%s => %s%n", strategyName, file.getFileName(), e.getMessage());
					}
				}
			}
		}

		try (PrintWriter summaryWriter = new PrintWriter(new FileWriter(summaryPath.toFile()))) {
			summaryWriter.println("benchmark,strategy,mean_throughput_tx_per_sec,mean_avg_latency_ms,samples");

			List<String> benchmarkOrder = new ArrayList<>(Arrays.asList("Courseware", "SmallBank", "TPCC"));
			for (String bench : benchmarkStats.keySet()) {
				if (!benchmarkOrder.contains(bench)) {
					benchmarkOrder.add(bench);
				}
			}
			if (benchmarkOrder.size() > 3) {
				List<String> tail = new ArrayList<>(benchmarkOrder.subList(3, benchmarkOrder.size()));
				Collections.sort(tail);
				benchmarkOrder = new ArrayList<>(benchmarkOrder.subList(0, 3));
				benchmarkOrder.addAll(tail);
			}

			for (String benchmark : benchmarkOrder) {
				Map<Executor.ExecutionStrategy, Stats> byStrategy = benchmarkStats.get(benchmark);
				if (byStrategy == null) {
					continue;
				}
				for (Executor.ExecutionStrategy strategy : strategies) {
					Stats s = byStrategy.get(strategy);
					if (s == null || s.count == 0) {
						continue;
					}
					summaryWriter.printf("%s,%s,%.6f,%.6f,%d%n",
							benchmark,
							displayName(strategy),
							s.meanThroughput(),
							s.meanLatency(),
							s.count);
				}
			}
		}

		VisualizationExporter.generateStrategyComparisonPng(summaryPath, pngPath);

		System.out.printf("Comparison finished. detail=%s summary=%s plot=%s failures=%d%n",
				detailPath,
				summaryPath,
				pngPath,
				failures);
	}

	private static String displayName(Executor.ExecutionStrategy strategy) {
		switch (strategy) {
			case ALL_SER:
				return "SER";
			case NON_SER_AS_SI:
				return "SI-SER";
			case CURRENT:
			default:
				return "PC-SI-SER";
		}
	}
}
