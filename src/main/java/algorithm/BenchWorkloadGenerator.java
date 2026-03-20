package algorithm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import model.core.IsolationLevel;
import model.core.OperationType;
import model.workload.ProgramInstance;
import model.workload.StaticOperation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class BenchWorkloadGenerator {
	private final int sessions;
	private final int txnsPerSession;
	private final int maxKey;
	private final int cases;

	private final Random random = new Random();
	private final ObjectMapper mapper = new ObjectMapper();

	public BenchWorkloadGenerator(int sessions, int txnsPerSession, int maxKey, int cases) {
		this.sessions = sessions;
		this.txnsPerSession = txnsPerSession;
		this.maxKey = maxKey;
		this.cases = cases;
	}

	public static void main(String[] args) throws Exception {
		Map<String, String> options = parseArgs(args);
		int sessions = Integer.parseInt(options.getOrDefault("--sessions", "3"));
		int txnsPerSession = Integer.parseInt(options.getOrDefault("--txns-per-session", "100"));
		int maxKey = Integer.parseInt(options.getOrDefault("--max-key", "50"));
		int cases = Integer.parseInt(options.getOrDefault("--cases", "3"));

		if (sessions <= 0 || txnsPerSession <= 0 || maxKey <= 0 || cases <= 0) {
			throw new IllegalArgumentException("sessions/txns-per-session/max-key/cases must be positive");
		}

		BenchWorkloadGenerator generator = new BenchWorkloadGenerator(sessions, txnsPerSession, maxKey, cases);
		generator.generate();
	}

	public void generate() throws IOException {
		Path projectDir = Paths.get(".").toAbsolutePath().normalize();
		Path benchmarkDir = projectDir.resolve("data").resolve("benchmarks");
		Path outputDir = projectDir.resolve("data").resolve("bench_workload");
		Files.createDirectories(outputDir);

		List<Path> benchmarkFiles = new ArrayList<>();
		Files.list(benchmarkDir)
				.filter(p -> p.getFileName().toString().endsWith(".json"))
				.sorted(Comparator.comparing(p -> p.getFileName().toString()))
				.forEach(benchmarkFiles::add);

		for (Path benchmarkFile : benchmarkFiles) {
			JsonNode benchmark = mapper.readTree(benchmarkFile.toFile());
			ArrayNode templates = (ArrayNode) benchmark.path("templates");
			if (templates == null || templates.isEmpty()) {
				continue;
			}

			String benchmarkName = stripExt(benchmarkFile.getFileName().toString());
			for (int caseNum = 1; caseNum <= cases; caseNum++) {
				random.setSeed(caseNum);
				List<ProgramInstance> txns = instantiateTransactions(templates, sessions * txnsPerSession);
				Collections.shuffle(txns, random);

				ArrayNode sessionsNode = mapper.createArrayNode();
				for (int sessionIdx = 0; sessionIdx < sessions; sessionIdx++) {
					int start = sessionIdx * txnsPerSession;
					int end = Math.min(txns.size(), start + txnsPerSession);
					if (start >= txns.size()) {
						break;
					}

					ObjectNode sessionNode = mapper.createObjectNode();
					sessionNode.put("id", sessionIdx + 1);
					sessionNode.set("instances", mapper.valueToTree(txns.subList(start, end)));
					sessionsNode.add(sessionNode);
				}

				ObjectNode workload = mapper.createObjectNode();
				workload.set("sessions", sessionsNode);

				String outputName = String.format("%s_%ds_%dt_%dk_%d.json", benchmarkName, sessions, txnsPerSession, maxKey, caseNum);
				mapper.writerWithDefaultPrettyPrinter().writeValue(outputDir.resolve(outputName).toFile(), workload);
				System.out.println("Generated " + outputName);
			}
		}
	}

	private List<ProgramInstance> instantiateTransactions(ArrayNode templates, int totalTxns) {
		List<WeightedTemplate> weighted = new ArrayList<>();
		for (int i = 0; i < templates.size(); i++) {
			JsonNode template = templates.get(i);
			double weight = template.path("percentage").asDouble(0.0);
			weighted.add(new WeightedTemplate(template, Math.max(0.0, weight), i));
		}

		double totalWeight = weighted.stream().mapToDouble(w -> w.weight).sum();
		if (totalWeight <= 0.0) {
			for (WeightedTemplate w : weighted) {
				w.weight = 1.0;
			}
			totalWeight = weighted.size();
		}

		int assigned = 0;
		for (WeightedTemplate w : weighted) {
			double exact = (w.weight / totalWeight) * totalTxns;
			w.baseCount = (int) Math.floor(exact);
			w.remainder = exact - w.baseCount;
			assigned += w.baseCount;
		}

		int remaining = totalTxns - assigned;
		weighted.sort((a, b) -> {
			int cmp = Double.compare(b.remainder, a.remainder);
			if (cmp != 0) {
				return cmp;
			}
			return Integer.compare(a.index, b.index);
		});
		for (int i = 0; i < remaining; i++) {
			weighted.get(i % weighted.size()).baseCount++;
		}

		weighted.sort(Comparator.comparingInt(w -> w.index));

		Map<String, Integer> nameCounter = new HashMap<>();
		List<ProgramInstance> txns = new ArrayList<>(totalTxns);
		for (WeightedTemplate w : weighted) {
			JsonNode template = w.template;
			String name = template.path("name").asText("unknown");
			IsolationLevel level = IsolationLevel.valueOf(template.path("isolationLevel").asText("SERIALIZABLE"));
			ArrayNode operations = (ArrayNode) template.path("operations");
			JsonNode paramsNode = template.path("params");

			for (int n = 0; n < w.baseCount; n++) {
				Map<String, Integer> paramValues = new HashMap<>();
				if (paramsNode.isArray()) {
					for (JsonNode p : paramsNode) {
						paramValues.put(p.asText(), random.nextInt(maxKey) + 1);
					}
				}

				List<StaticOperation> concreteOps = instantiateOps(operations, paramValues);
				int seq = nameCounter.getOrDefault(name, 0) + 1;
				nameCounter.put(name, seq);
				txns.add(new ProgramInstance(name + "_" + seq, level, concreteOps));
			}
		}

		if (txns.size() != totalTxns) {
			throw new IllegalStateException("Strict ratio allocation failed: expected " + totalTxns + " but got " + txns.size());
		}

		return txns;
	}

	private List<StaticOperation> instantiateOps(ArrayNode ops, Map<String, Integer> paramValues) {
		List<StaticOperation> result = new ArrayList<>();
		for (JsonNode op : ops) {
			int id = op.path("id").asInt();
			String type = op.path("type").asText();
			String table = op.path("key").asText();

			String concreteKey = table;
			JsonNode params = op.get("params");
			if (params != null) {
				List<String> parts = new ArrayList<>();
				if (params.isArray()) {
					for (JsonNode p : params) {
						String k = p.asText();
						parts.add(String.valueOf(paramValues.getOrDefault(k, random.nextInt(maxKey) + 1)));
					}
				} else {
					String k = params.asText();
					parts.add(String.valueOf(paramValues.getOrDefault(k, random.nextInt(maxKey) + 1)));
				}
				concreteKey = table + "_" + String.join("_", parts);
			} else {
				concreteKey = table + "_" + (random.nextInt(maxKey) + 1);
			}

			if ("UPDATE".equals(type)) {
				result.add(new StaticOperation(id, OperationType.READ, concreteKey, -1));
				result.add(new StaticOperation(id, OperationType.WRITE, concreteKey, -1));
			} else {
				result.add(new StaticOperation(id, OperationType.valueOf(type), concreteKey, -1));
			}
		}
		return result;
	}

	private static String stripExt(String fileName) {
		int idx = fileName.lastIndexOf('.');
		return idx > 0 ? fileName.substring(0, idx) : fileName;
	}

	private static Map<String, String> parseArgs(String[] args) {
		Map<String, String> options = new HashMap<>();
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.startsWith("--") && i + 1 < args.length) {
				options.put(arg, args[++i]);
			}
		}
		return options;
	}

	private static class WeightedTemplate {
		JsonNode template;
		double weight;
		double remainder;
		int baseCount;
		int index;

		WeightedTemplate(JsonNode template, double weight, int index) {
			this.template = template;
			this.weight = weight;
			this.remainder = 0.0;
			this.baseCount = 0;
			this.index = index;
		}
	}
}
