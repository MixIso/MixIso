package algorithm;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.workload.ProgramInstance;
import model.workload.TemplateSet;

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

public class BenchWorkloadAllocatorBatch {
	public static void main(String[] args) throws Exception {
		Path projectDir = Paths.get(".").toAbsolutePath().normalize();
		Path inputDir = projectDir.resolve("data").resolve("bench_workload");
		Path outputDir = projectDir.resolve("data").resolve("allocated_bench_workload");
		Path distCsv = projectDir.resolve("data").resolve("bench_allocation_distribution.csv");
		Path distPng = projectDir.resolve("data").resolve("bench_allocation_distribution.png");

		Files.createDirectories(outputDir);

		List<Path> files = new ArrayList<>();
		Files.list(inputDir)
				.filter(p -> p.getFileName().toString().endsWith(".json"))
				.sorted(Comparator.comparing(p -> p.getFileName().toString()))
				.forEach(files::add);

		if (files.isEmpty()) {
			System.out.println("No workload files found in " + inputDir);
			return;
		}

		Map<String, Map<String, Double>> distribution = new HashMap<>();
		int ok = 0;
		int fail = 0;
		for (Path input : files) {
			String name = input.getFileName().toString();
			Path output = outputDir.resolve(name);
			try {
				Allocator.allocateAndSave(input.toString(), output.toString());
				ok++;
				System.out.println("[OK] " + name);

				Map<String, Double> dist = computeDistribution(output);
				distribution.put(name, dist);
			} catch (Exception e) {
				fail++;
				System.out.println("[FAIL] " + name + " => " + e.getMessage());
			}
		}

		writeDistributionCsv(distCsv, distribution);
		VisualizationExporter.generateBenchDistributionPng(distCsv, distPng);
		System.out.println("Distribution chart written to " + distPng);
		System.out.println("Done. success=" + ok + " failed=" + fail);
	}

	private static Map<String, Double> computeDistribution(Path workloadFile) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		TemplateSet set = mapper.readValue(workloadFile.toFile(), TemplateSet.class);
		List<ProgramInstance> templates = set.getTemplates();

		Map<String, Integer> counts = new HashMap<>();
		counts.put("SER", 0);
		counts.put("SI", 0);
		counts.put("PSI", 0);
		counts.put("PC", 0);
		counts.put("RA", 0);

		for (ProgramInstance t : templates) {
			String level = t.getIsolationLevel().name();
			if ("SERIALIZABLE".equals(level)) {
				counts.put("SER", counts.get("SER") + 1);
			} else if ("SNAPSHOT_ISOLATION".equals(level)) {
				counts.put("SI", counts.get("SI") + 1);
			} else if ("PARALLEL_SNAPSHOT_ISOLATION".equals(level)) {
				counts.put("PSI", counts.get("PSI") + 1);
			} else if ("PREFIX_CONSISTENCY".equals(level)) {
				counts.put("PC", counts.get("PC") + 1);
			} else {
				counts.put("RA", counts.get("RA") + 1);
			}
		}

		double total = Math.max(1, templates.size());
		Map<String, Double> out = new HashMap<>();
		for (Map.Entry<String, Integer> entry : counts.entrySet()) {
			out.put(entry.getKey(), entry.getValue() * 100.0 / total);
		}
		return out;
	}

	private static void writeDistributionCsv(Path csvPath, Map<String, Map<String, Double>> distribution) throws Exception {
		try (PrintWriter writer = new PrintWriter(new FileWriter(csvPath.toFile()))) {
			writer.println("file,SER,SI,PSI,PC,RA");
			distribution.entrySet().stream()
					.sorted(Map.Entry.comparingByKey())
					.forEach(e -> {
						Map<String, Double> d = e.getValue();
						writer.printf("%s,%.2f,%.2f,%.2f,%.2f,%.2f%n",
								e.getKey(),
								d.getOrDefault("SER", 0.0),
								d.getOrDefault("SI", 0.0),
								d.getOrDefault("PSI", 0.0),
								d.getOrDefault("PC", 0.0),
								d.getOrDefault("RA", 0.0));
					});
		}
	}
}
