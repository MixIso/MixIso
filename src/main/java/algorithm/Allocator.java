package algorithm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import model.core.IsolationLevel;
import model.workload.ProgramInstance;
import model.workload.TemplateSet;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class Allocator {
	public static List<ProgramInstance> allocate(List<ProgramInstance> templates) {
		return templates.stream().map(template -> {
			if (template.isWriteOnly() || template.isSingleRead()) {
				return new ProgramInstance(template.getName(), IsolationLevel.READ_ATOMIC, template.getOperations());
			}

			if (template.isReadOnly()) {
				return new ProgramInstance(template.getName(), IsolationLevel.PREFIX_CONSISTENCY, template.getOperations());
			}

			for (ProgramInstance other : templates) {
				if (template != other) {
					boolean RWConflict = !template.getReadSet().stream()
							.filter(key -> other.getWriteSet().contains(key))
							.collect(Collectors.toSet())
							.isEmpty();

					boolean WWConflict = !template.getWriteSet().stream()
							.filter(key -> other.getWriteSet().contains(key))
							.collect(Collectors.toSet())
							.isEmpty();

					if (RWConflict && !WWConflict) {
						return new ProgramInstance(template.getName(), IsolationLevel.SERIALIZABLE, template.getOperations());
					}
				}
			}

			return new ProgramInstance(template.getName(), IsolationLevel.PARALLEL_SNAPSHOT_ISOLATION, template.getOperations());
		}).collect(Collectors.toList());
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage:");
			System.err.println("  Mode 1 - Benchmark:     Allocator benchmark <workload_file> <output_csv> [warmups] [iterations]");
			System.err.println("  Mode 2 - Allocate:      Allocator allocate <input_workload> <output_workload>");
			System.err.println("");
			System.err.println("Examples:");
			System.err.println("  Allocator benchmark data/workload/workload_100t_5o_50k_50r_1.json results.csv 3 10");
			System.err.println("  Allocator allocate data/bench_workload/SmallBank-1.json data/allocated_bench_workload/SmallBank-1-allocated.json");
			return;
		}

		String mode = args[0].toLowerCase();
		
		try {
			if (mode.equals("benchmark")) {
				if (args.length < 3) {
					System.err.println("Benchmark mode requires: Allocator benchmark <workload_file> <output_csv> [warmups] [iterations]");
					return;
				}
				String workloadFile = args[1];
				String outputCsv = args[2];
				int warmups = args.length >= 4 ? Integer.parseInt(args[3]) : 3;
				int iterations = args.length >= 5 ? Integer.parseInt(args[4]) : 10;
				benchmark(workloadFile, outputCsv, warmups, iterations);
			} else if (mode.equals("allocate")) {
				if (args.length < 3) {
					System.err.println("Allocate mode requires: Allocator allocate <input_workload> <output_workload>");
					return;
				}
				String inputFile = args[1];
				String outputFile = args[2];
				allocateAndSave(inputFile, outputFile);
			} else {
				System.err.println("Unknown mode: " + mode);
				System.err.println("Supported modes: benchmark, allocate");
			}
		} catch (Exception e) {
			System.err.println("Error during execution: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * Reads a workload file, applies allocation algorithm, and saves the result to JSON.
	 *
	 * @param inputFile  path to input workload JSON file
	 * @param outputFile path to output allocated workload JSON file
	 * @throws IOException if file operations fail
	 */
	public static void allocateAndSave(String inputFile, String outputFile) throws IOException {
		// Read input file
		Path inputPath = Paths.get(inputFile);
		if (!Files.exists(inputPath)) {
			System.err.println("Input file does not exist: " + inputFile);
			return;
		}

		// Parse JSON to TemplateSet
		ObjectMapper mapper = new ObjectMapper();
		TemplateSet templateSet = mapper.readValue(inputPath.toFile(), TemplateSet.class);
		List<ProgramInstance> templates = templateSet.getTemplates();

		if (templates == null || templates.isEmpty()) {
			System.err.println("No templates found in: " + inputFile);
			return;
		}

		// Apply allocation algorithm
		List<ProgramInstance> allocatedTemplates = allocate(templates);

		// Create output directory if it doesn't exist
		Path outputPath = Paths.get(outputFile);
		Files.createDirectories(outputPath.getParent());

		// Save allocated workload to JSON file
		TemplateSet allocatedSet = new TemplateSet(allocatedTemplates);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String jsonString = gson.toJson(allocatedSet);
		Files.write(outputPath, jsonString.getBytes());

		System.out.println("Allocation completed successfully.");
		System.out.println("  Input:  " + inputFile);
		System.out.println("  Output: " + outputFile);
		System.out.println("  Allocated " + allocatedTemplates.size() + " templates");
	}

	private static void benchmark(String workloadFile, String outputCsv, int warmups, int iterations) throws IOException {
		Path inputPath = Paths.get(workloadFile);
		if (!Files.exists(inputPath)) {
			System.err.println("Workload file does not exist: " + workloadFile);
			return;
		}

		WorkloadParams params = parseParamsFromFilename(inputPath.getFileName().toString());

		ObjectMapper mapper = new ObjectMapper();
		TemplateSet templateSet = mapper.readValue(inputPath.toFile(), TemplateSet.class);
		List<ProgramInstance> templates = templateSet.getTemplates();

		for (int i = 0; i < warmups; i++) {
			allocate(templates);
		}

		long totalTimeNs = 0L;
		for (int i = 0; i < iterations; i++) {
			long start = System.nanoTime();
			allocate(templates);
			totalTimeNs += (System.nanoTime() - start);
		}

		double avgTimeMs = (totalTimeNs / (double) iterations) / 1_000_000.0;
		writeCsv(outputCsv, params, avgTimeMs);
		System.out.printf("Benchmark completed: %s -> %.4f ms%n", inputPath.getFileName(), avgTimeMs);
	}

	private static WorkloadParams parseParamsFromFilename(String fileName) {
		String[] parts = fileName.replace("workload_", "").replace(".json", "").split("_");
		int txnCount = Integer.parseInt(parts[0].replace("t", ""));
		int opPerTxn = Integer.parseInt(parts[1].replace("o", ""));
		int maxKey = Integer.parseInt(parts[2].replace("k", ""));
		int readOnlyPercent = Integer.parseInt(parts[3].replace("r", ""));
		int caseNum = Integer.parseInt(parts[4]);
		return new WorkloadParams(txnCount, opPerTxn, maxKey, readOnlyPercent, caseNum);
	}

	private static void writeCsv(String outputCsv, WorkloadParams params, double avgTimeMs) throws IOException {
		Path outputPath = Paths.get(outputCsv);
		boolean fileExists = Files.exists(outputPath);
		try (PrintWriter writer = new PrintWriter(new FileWriter(outputCsv, true))) {
			if (!fileExists) {
				writer.println("txn_count,op_per_txn,max_key,read_only_percent,case_num,avg_time_ms");
			}
			writer.printf("%d,%d,%d,%d,%d,%.4f%n", params.txnCount, params.opPerTxn,
					params.maxKey, params.readOnlyPercent, params.caseNum, avgTimeMs);
		}
	}

	private static class WorkloadParams {
		final int txnCount;
		final int opPerTxn;
		final int maxKey;
		final int readOnlyPercent;
		final int caseNum;

		WorkloadParams(int txnCount, int opPerTxn, int maxKey, int readOnlyPercent, int caseNum) {
			this.txnCount = txnCount;
			this.opPerTxn = opPerTxn;
			this.maxKey = maxKey;
			this.readOnlyPercent = readOnlyPercent;
			this.caseNum = caseNum;
		}
	}
}