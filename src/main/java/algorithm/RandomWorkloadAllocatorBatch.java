package algorithm;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RandomWorkloadAllocatorBatch {
	public static void main(String[] args) throws Exception {
		Path projectDir = Paths.get(".").toAbsolutePath().normalize();
		Path inputDir = projectDir.resolve("data").resolve("random_workload");
		Path outputDir = projectDir.resolve("data").resolve("allocated_random_workload");
		Path perfCsv = projectDir.resolve("data").resolve("allocation_performance.csv");
		Path analysisCsv = projectDir.resolve("data").resolve("allocation_performance_analysis.csv");
		Path perfPng = projectDir.resolve("data").resolve("allocation_performance.png");

		Files.createDirectories(outputDir);
		if (perfCsv.getParent() != null) {
			Files.createDirectories(perfCsv.getParent());
		}

		List<Path> files = new ArrayList<>();
		Files.list(inputDir)
				.filter(p -> p.getFileName().toString().endsWith(".json"))
				.sorted(Comparator.comparing(p -> p.getFileName().toString()))
				.forEach(files::add);

		if (files.isEmpty()) {
			System.out.println("No random workload files found in " + inputDir);
			return;
		}

		try (PrintWriter writer = new PrintWriter(new FileWriter(perfCsv.toFile()))) {
			writer.println("filename,status,execution_time_seconds,error");
			for (Path input : files) {
				Path output = outputDir.resolve(input.getFileName().toString());
				long startNs = System.nanoTime();
				try {
					Allocator.allocateAndSave(input.toString(), output.toString());
					double seconds = (System.nanoTime() - startNs) / 1_000_000_000.0;
					writer.printf("%s,success,%.6f,%s%n", input.getFileName(), seconds, "");
					System.out.printf("[OK] %s %.3fs%n", input.getFileName(), seconds);
				} catch (Exception e) {
					double seconds = (System.nanoTime() - startNs) / 1_000_000_000.0;
					String err = e.getMessage() == null ? "unknown" : e.getMessage().replace(",", ";");
					writer.printf("%s,failed,%.6f,%s%n", input.getFileName(), seconds, err);
					System.out.printf("[FAIL] %s %.3fs %s%n", input.getFileName(), seconds, err);
				}
			}
		}

		System.out.println("Performance CSV written to " + perfCsv);
		VisualizationExporter.generateRandomAnalysisAndPng(perfCsv, analysisCsv, perfPng);
		System.out.println("Performance analysis CSV written to " + analysisCsv);
		System.out.println("Performance chart written to " + perfPng);
	}
}
