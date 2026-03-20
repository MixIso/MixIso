package algorithm;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.core.IsolationLevel;
import model.core.OperationType;
import model.workload.ProgramInstance;
import model.workload.StaticOperation;
import model.workload.TemplateSet;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomWorkloadGenerator {
	private final int totalTxns;
	private final int maxOps;
	private final int maxKey;
	private final int cases;
	private final int readOnlyPercent;
	private final Random random = new Random();

	public RandomWorkloadGenerator(int totalTxns, int maxOps, int maxKey, int cases, int readOnlyPercent) {
		this.totalTxns = totalTxns;
		this.maxOps = maxOps;
		this.maxKey = maxKey;
		this.cases = cases;
		this.readOnlyPercent = readOnlyPercent;
	}

	public static void main(String[] args) throws Exception {
		Map<String, String> options = parseArgs(args);
		int txns = Integer.parseInt(options.getOrDefault("--txns", "1000"));
		int maxOps = Integer.parseInt(options.getOrDefault("--max-ops", "10"));
		int maxKey = Integer.parseInt(options.getOrDefault("--max-key", "500000"));
		int cases = Integer.parseInt(options.getOrDefault("--cases", "1"));
		int readOnly = Integer.parseInt(options.getOrDefault("--read-only", "0"));

		RandomWorkloadGenerator generator = new RandomWorkloadGenerator(txns, maxOps, maxKey, cases, readOnly);
		generator.generate();
	}

	public void generate() throws Exception {
		Path projectDir = Paths.get(".").toAbsolutePath().normalize();
		Path outputDir = projectDir.resolve("data").resolve("random_workload");
		Files.createDirectories(outputDir);
		ObjectMapper mapper = new ObjectMapper();

		for (int caseNum = 1; caseNum <= cases; caseNum++) {
			random.setSeed(caseNum);
			List<ProgramInstance> txns = new ArrayList<>();
			for (int i = 1; i <= totalTxns; i++) {
				boolean readOnly = random.nextInt(100) < readOnlyPercent;
				List<StaticOperation> ops = readOnly ? genReadOnlyOps() : genMixedOps();
				txns.add(new ProgramInstance("Txn_" + i, IsolationLevel.SERIALIZABLE, ops));
			}

			Collections.shuffle(txns, random);
			TemplateSet workload = new TemplateSet(txns);
			String fileName = String.format("workload_%dt_%do_%dk_%dr_%d.json", totalTxns, maxOps, maxKey, readOnlyPercent, caseNum);
			mapper.writerWithDefaultPrettyPrinter().writeValue(outputDir.resolve(fileName).toFile(), workload);
			System.out.println("Generated " + fileName);
		}
	}

	private List<StaticOperation> genReadOnlyOps() {
		int opCount = random.nextInt(maxOps) + 1;
		List<StaticOperation> ops = new ArrayList<>();
		for (int i = 1; i <= opCount; i++) {
			ops.add(new StaticOperation(i, OperationType.READ, "key_" + (random.nextInt(maxKey) + 1), -1));
		}
		return ops;
	}

	private List<StaticOperation> genMixedOps() {
		int opCount = random.nextInt(maxOps) + 1;
		List<StaticOperation> ops = new ArrayList<>();
		for (int i = 1; i <= opCount; i++) {
			OperationType type = random.nextBoolean() ? OperationType.READ : OperationType.WRITE;
			ops.add(new StaticOperation(i, type, "key_" + (random.nextInt(maxKey) + 1), -1));
		}
		return ops;
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
}
