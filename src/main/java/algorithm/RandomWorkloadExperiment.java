package algorithm;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RandomWorkloadExperiment {
	public static void main(String[] args) throws Exception {
		Path projectDir = Paths.get(".").toAbsolutePath().normalize();
		Path summaryCsv = projectDir.resolve("data").resolve("allocation_experiment_summary.csv");
		if (summaryCsv.getParent() != null) {
			Files.createDirectories(summaryCsv.getParent());
		}

		Map<String, Integer> base = new LinkedHashMap<>();
		base.put("txns", 500);
		base.put("maxOps", 10);
		base.put("maxKey", 500000);
		base.put("readOnly", 0);
		int cases = 5;

		try (PrintWriter writer = new PrintWriter(new FileWriter(summaryCsv.toFile()))) {
			writer.println("experiment,vary_param,vary_value,status");

			runSeries("vary_txns", "txns", Arrays.asList(100, 300, 500, 800, 1000), base, cases, writer);
			runSeries("vary_max_ops", "maxOps", Arrays.asList(2, 5, 10, 15), base, cases, writer);
			runSeries("vary_max_key", "maxKey", Arrays.asList(1000, 10000, 100000, 500000), base, cases, writer);
		}

		System.out.println("Experiment summary written to " + summaryCsv);
	}

	private static void runSeries(String experiment,
								String varyParam,
								List<Integer> values,
								Map<String, Integer> base,
								int cases,
								PrintWriter writer) throws Exception {
		for (Integer value : values) {
			int txns = varyParam.equals("txns") ? value : base.get("txns");
			int maxOps = varyParam.equals("maxOps") ? value : base.get("maxOps");
			int maxKey = varyParam.equals("maxKey") ? value : base.get("maxKey");
			int readOnly = varyParam.equals("readOnly") ? value : base.get("readOnly");

			try {
				cleanDir(Paths.get("data").resolve("random_workload"));
				cleanDir(Paths.get("data").resolve("allocated_random_workload"));

				RandomWorkloadGenerator generator = new RandomWorkloadGenerator(txns, maxOps, maxKey, cases, readOnly);
				generator.generate();
				RandomWorkloadAllocatorBatch.main(new String[]{});

				writer.printf("%s,%s,%d,success%n", experiment, varyParam, value);
				System.out.printf("[OK] %s %s=%d%n", experiment, varyParam, value);
			} catch (Exception e) {
				writer.printf("%s,%s,%d,failed:%s%n", experiment, varyParam, value,
						e.getMessage() == null ? "unknown" : e.getMessage().replace(",", ";"));
				System.out.printf("[FAIL] %s %s=%d => %s%n", experiment, varyParam, value, e.getMessage());
			}
		}
	}

	private static void cleanDir(Path relativeDir) throws Exception {
		Path dir = Paths.get(".").toAbsolutePath().normalize().resolve(relativeDir);
		if (!Files.exists(dir)) {
			Files.createDirectories(dir);
			return;
		}
		Files.list(dir)
				.filter(p -> p.getFileName().toString().endsWith(".json"))
				.forEach(p -> {
					try {
						Files.deleteIfExists(p);
					} catch (Exception ignored) {
					}
				});
	}
}
