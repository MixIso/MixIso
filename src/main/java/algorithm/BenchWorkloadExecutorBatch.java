package algorithm;

import model.execution.ExecutionResult;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchWorkloadExecutorBatch {
	public static void main(String[] args) throws Exception {
		int dcCount = args.length >= 1 ? Integer.parseInt(args[0]) : 5;
		int minRtt = args.length >= 2 ? Integer.parseInt(args[1]) : 3;
		int maxRtt = args.length >= 3 ? Integer.parseInt(args[2]) : 10;
		String outputCsv = args.length >= 4 ? args[3] : "data/bench_execution_results.csv";

		Path projectDir = Paths.get(".").toAbsolutePath().normalize();
		Path allocatedDir = projectDir.resolve("data").resolve("allocated_bench_workload");
		Path csvPath = projectDir.resolve(outputCsv);
		if (csvPath.getParent() != null) {
			Files.createDirectories(csvPath.getParent());
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

		int ok = 0;
		int fail = 0;
		for (int i = 0; i < files.size(); i++) {
			Path file = files.get(i);
			try {
				System.out.printf("[RUN] (%d/%d) %s%n", i + 1, files.size(), file.getFileName());
				ExecutionResult result = Executor.runSingleWorkload(file.toString(), csvPath.toString(), dcCount, minRtt, maxRtt);
				ok++;
				System.out.printf("[OK] %s committed=%d aborted=%d throughput=%.2f%n",
						file.getFileName(),
						result.getCommittedTxns(),
						result.getAbortedTxns(),
						result.getThroughputTxPerSec());
			} catch (Exception e) {
				fail++;
				System.out.println("[FAIL] " + file.getFileName() + " => " + e.getMessage());
			}
		}

		System.out.println("Execution finished. success=" + ok + " failed=" + fail + " output=" + csvPath);

		// Generate aggregated summary and visualization
		Path summaryDir = csvPath.getParent();
		Path summaryCsv = summaryDir.resolve("bench_execution_summary.csv");
		Path summaryPng = summaryDir.resolve("bench_execution_summary.png");
		
		try {
			aggregateAndVisualize(csvPath, summaryCsv, summaryPng);
			System.out.println("Summary chart written to " + summaryPng);
		} catch (Exception e) {
			System.out.println("Warning: Could not generate visualization => " + e.getMessage());
		}
	}

	private static void aggregateAndVisualize(Path detailCsv, Path summaryCsv, Path summaryPng) throws Exception {
		// Read detail CSV and aggregate by benchmark
		// Columns: file,benchmark,sessions,txns_per_session,max_key,case_num,total_txns,committed,aborted,throughput_tx_per_sec,avg_latency_ms,...
		Map<String, List<double[]>> benchmarkData = new HashMap<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(detailCsv.toFile()))) {
			String line = reader.readLine(); // skip header
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split(",");
				if (parts.length >= 11) {
					String benchmark = parts[1].trim();  // column 1
					double throughput = Double.parseDouble(parts[9].trim());  // column 9: throughput_tx_per_sec
					double latency = Double.parseDouble(parts[10].trim());    // column 10: avg_latency_ms
					
					benchmarkData.computeIfAbsent(benchmark, k -> new ArrayList<>()).add(new double[]{throughput, latency});
				}
			}
		}

		// Calculate means and write summary CSV
		try (PrintWriter writer = new PrintWriter(new FileWriter(summaryCsv.toFile()))) {
			writer.println("benchmark,mean_throughput_tx_per_sec,mean_avg_latency_ms,samples");
			
			List<String> benchmarkOrder = new ArrayList<>();
			benchmarkOrder.add("Courseware");
			benchmarkOrder.add("SmallBank");
			benchmarkOrder.add("TPCC");
			
			for (String benchmark : benchmarkOrder) {
				List<double[]> data = benchmarkData.get(benchmark);
				if (data != null && !data.isEmpty()) {
					double meanThroughput = data.stream().mapToDouble(d -> d[0]).average().orElse(0.0);
					double meanLatency = data.stream().mapToDouble(d -> d[1]).average().orElse(0.0);
					writer.printf("%s,%.2f,%.2f,%d%n", benchmark, meanThroughput, meanLatency, data.size());
				}
			}
		}

		// Generate visualization
		VisualizationExporter.generateExecutionSummaryPng(summaryCsv, summaryPng);
	}

	private static String extractBenchmarkName(String filename) {
		if (filename.contains("Courseware")) return "Courseware";
		if (filename.contains("SmallBank")) return "SmallBank";
		if (filename.contains("TPCC")) return "TPCC";
		// fallback: extract first part before underscore
		String[] parts = filename.split("_");
		return parts.length > 0 ? parts[0] : filename;
	}
}
