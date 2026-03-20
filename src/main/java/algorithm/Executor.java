package algorithm;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.core.IsolationLevel;
import model.execution.CommitRecord;
import model.execution.DataCenterNode;
import model.execution.ExecutionResult;
import model.execution.TxContext;
import model.workload.ProgramInstance;
import model.workload.StaticOperation;
import model.workload.TemplateSet;
import model.workload.WorkloadMeta;
import model.workload.WorkloadSession;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Executor {
	public enum ExecutionStrategy {
		CURRENT,
		NON_SER_AS_SI,
		ALL_SER;

		public static ExecutionStrategy fromString(String raw) {
			if (raw == null || raw.trim().isEmpty()) {
				return CURRENT;
			}
			String normalized = raw.trim().toUpperCase();
			for (ExecutionStrategy strategy : values()) {
				if (strategy.name().equals(normalized)) {
					return strategy;
				}
			}
			throw new IllegalArgumentException("Unknown strategy: " + raw + ". Supported: CURRENT, NON_SER_AS_SI, ALL_SER");
		}
	}

	private static final int DEFAULT_DC_COUNT = 5;
	private static final int DEFAULT_MIN_RTT_MS = 1;
	private static final int DEFAULT_MAX_RTT_MS = 3;
	private static final long TX_TIMEOUT_SECONDS = 120;
	private static final int ARBITER_NODE_ID = -1;

	private final List<DataCenterNode> dataCenters;
	private final int minRttMs;
	private final int maxRttMs;
	private final AtomicLong logicalClock;
	private final AtomicLong txnIdGenerator;
	private final CopyOnWriteArrayList<CommitRecord> commitLog;
	private final Set<Long> committedTransactions;
	private final Map<Long, Set<Long>> visibleToTxn;
	private final Map<Long, Long> commitTimestampByTxn;
	private final Map<Integer, Set<Long>> replicaVisibleTransactions;
	private final Random rttRandom;
	private final ExecutionStrategy strategy;
	private final Object godLock;
	
	// ThreadLocal assigns a data center to each thread
	private final ThreadLocal<Integer> threadDataCenterMap = new ThreadLocal<>();

	private Executor(int dcCount, int minRttMs, int maxRttMs, ExecutionStrategy strategy) {
		this.minRttMs = minRttMs;
		this.maxRttMs = maxRttMs;
		this.strategy = strategy;
		this.logicalClock = new AtomicLong(0L);
		this.txnIdGenerator = new AtomicLong(0L);
		this.commitLog = new CopyOnWriteArrayList<>();
		this.godLock = new Object();
		this.committedTransactions = ConcurrentHashMap.newKeySet();
		this.visibleToTxn = new ConcurrentHashMap<>();
		this.commitTimestampByTxn = new ConcurrentHashMap<>();
		this.replicaVisibleTransactions = new ConcurrentHashMap<>();
		this.rttRandom = new Random(42L);
		this.dataCenters = new ArrayList<>();
		for (int i = 0; i < dcCount; i++) {
			dataCenters.add(new DataCenterNode(i));
			replicaVisibleTransactions.put(i, ConcurrentHashMap.newKeySet());
		}
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: Executor <allocated_workload_file> <output_csv> [dcCount] [minRttMs] [maxRttMs] [strategy]");
			System.err.println("Example: Executor data/allocated_bench_workload/Courseware_10s_20t_500k_1.json data/bench_execution_results.csv 5 100 300 CURRENT");
			System.exit(1);
		}

		String workloadFile = args[0];
		String outputCsv = args[1];
		int dcCount = args.length >= 3 ? Integer.parseInt(args[2]) : DEFAULT_DC_COUNT;
		int minRtt = args.length >= 4 ? Integer.parseInt(args[3]) : DEFAULT_MIN_RTT_MS;
		int maxRtt = args.length >= 5 ? Integer.parseInt(args[4]) : DEFAULT_MAX_RTT_MS;
		ExecutionStrategy strategy = args.length >= 6 ? ExecutionStrategy.fromString(args[5]) : ExecutionStrategy.CURRENT;

		if (dcCount <= 0 || minRtt < 0 || maxRtt < minRtt) {
			System.err.println("Invalid parameters: dcCount > 0, minRtt >= 0, maxRtt >= minRtt are required");
			System.exit(1);
		}

		Executor engine = new Executor(dcCount, minRtt, maxRtt, strategy);
		try {
			ExecutionResult result = engine.executeWorkloadFile(workloadFile);
			appendCsv(outputCsv, result);
			System.out.printf("Execution completed [%s]: %s | committed=%d aborted=%d throughput=%.2f tx/s avgLatency=%.2f ms%n",
					strategy,
					result.getFileName(), result.getCommittedTxns(), result.getAbortedTxns(), result.getThroughputTxPerSec(), result.getAvgLatencyMs());
		} catch (Exception e) {
			System.err.println("Execution failed: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		} finally {
			engine.shutdown();
		}
	}

	public static ExecutionResult runSingleWorkload(String workloadFile,
													 String outputCsv,
													 int dcCount,
													 int minRtt,
													 int maxRtt) throws Exception {
		return runSingleWorkload(workloadFile, outputCsv, dcCount, minRtt, maxRtt, ExecutionStrategy.CURRENT);
	}

	public static ExecutionResult runSingleWorkload(String workloadFile,
												 String outputCsv,
												 int dcCount,
												 int minRtt,
												 int maxRtt,
												 ExecutionStrategy strategy) throws Exception {
		if (dcCount <= 0 || minRtt < 0 || maxRtt < minRtt) {
			throw new IllegalArgumentException("Invalid parameters: dcCount > 0, minRtt >= 0, maxRtt >= minRtt are required");
		}

		Executor engine = new Executor(dcCount, minRtt, maxRtt, strategy);
		try {
			ExecutionResult result = engine.executeWorkloadFile(workloadFile);
			if (outputCsv != null && !outputCsv.trim().isEmpty()) {
				appendCsv(outputCsv, result);
			}
			return result;
		} finally {
			engine.shutdown();
		}
	}

	private static void appendCsv(String outputCsv, ExecutionResult result) throws IOException {
		Path outputPath = Paths.get(outputCsv);
		Path parent = outputPath.getParent();
		if (parent != null) {
			Files.createDirectories(parent);
		}

		boolean fileExists = Files.exists(outputPath);
		try (PrintWriter writer = new PrintWriter(new FileWriter(outputCsv, true))) {
			if (!fileExists) {
				writer.println("file,benchmark,sessions,txns_per_session,max_key,case_num,total_txns,committed,aborted,throughput_tx_per_sec,avg_latency_ms,p95_latency_ms,dc_count,min_rtt_ms,max_rtt_ms");
			}
			writer.printf("%s,%s,%d,%d,%d,%d,%d,%d,%d,%.4f,%.4f,%.4f,%d,%d,%d%n",
					result.getFileName(),
					result.getBenchmark(),
					result.getSessions(),
					result.getTxnsPerSession(),
					result.getMaxKey(),
					result.getCaseNum(),
					result.getTotalTxns(),
					result.getCommittedTxns(),
					result.getAbortedTxns(),
					result.getThroughputTxPerSec(),
					result.getAvgLatencyMs(),
					result.getP95LatencyMs(),
					result.getDcCount(),
					result.getMinRttMs(),
					result.getMaxRttMs());
		}
	}

	private ExecutionResult executeWorkloadFile(String workloadFile) throws IOException, InterruptedException {
		Path workloadPath = Paths.get(workloadFile);
		if (!Files.exists(workloadPath)) {
			throw new IOException("Workload file not found: " + workloadFile);
		}

		ObjectMapper mapper = new ObjectMapper();
		TemplateSet templateSet = mapper.readValue(workloadPath.toFile(), TemplateSet.class);

		List<WorkloadSession> sessionWorkloads = extractSessions(templateSet, workloadPath.getFileName().toString());
		if (sessionWorkloads.isEmpty()) {
			throw new IllegalStateException("No transactions found in workload: " + workloadFile);
		}

		for (DataCenterNode dataCenter : dataCenters) {
			dataCenter.seedInitialData(sessionWorkloads);
		}

		long executionStartMs = System.currentTimeMillis();
		List<Long> latencies = Collections.synchronizedList(new ArrayList<Long>());
		AtomicLong committed = new AtomicLong(0L);
		AtomicLong aborted = new AtomicLong(0L);

		ExecutorService sessionExecutor = Executors.newFixedThreadPool(sessionWorkloads.size());
		List<Callable<Void>> tasks = new ArrayList<>();
		for (int index = 0; index < sessionWorkloads.size(); index++) {
			WorkloadSession session = sessionWorkloads.get(index);
			int dataCenterId = index % dataCenters.size();
			tasks.add(() -> {
				executeSession(session, dataCenterId, latencies, committed, aborted);
				return null;
			});
		}

		try {
			List<Future<Void>> futures = sessionExecutor.invokeAll(tasks, TX_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			for (Future<Void> future : futures) {
				if (!future.isCancelled()) {
					try {
						future.get();
					} catch (ExecutionException e) {
						throw new IllegalStateException("Session execution error", e.getCause());
					}
				} else {
					throw new TimeoutException("Session execution timed out");
				}
			}
		} catch (TimeoutException e) {
			throw new IllegalStateException(e.getMessage(), e);
		} finally {
			sessionExecutor.shutdownNow();
		}

		long executionEndMs = System.currentTimeMillis();
		long wallMs = Math.max(1L, executionEndMs - executionStartMs);

		double avgLatency = latencies.stream().mapToLong(v -> v).average().orElse(0.0);
		double p95Latency = percentile(latencies, 95.0);
		double throughput = (committed.get() * 1000.0) / wallMs;

		WorkloadMeta meta = WorkloadMeta.fromFilename(workloadPath.getFileName().toString(), sessionWorkloads);

		return new ExecutionResult(
				workloadPath.getFileName().toString(),
				meta.getBenchmark(),
				meta.getSessions(),
				meta.getTxnsPerSession(),
				meta.getMaxKey(),
				meta.getCaseNum(),
				meta.getTotalTxns(),
				committed.get(),
				aborted.get(),
				throughput,
				avgLatency,
				p95Latency,
				dataCenters.size(),
				minRttMs,
				maxRttMs);
	}

	private void executeSession(WorkloadSession session,
								int assignedDataCenterId,
								List<Long> latencies,
								AtomicLong committed,
								AtomicLong aborted) {
		// Assign a data center to the current thread
		threadDataCenterMap.set(assignedDataCenterId);

		List<ProgramInstance> transactions = session.getInstances();
		if (transactions == null || transactions.isEmpty()) {
			threadDataCenterMap.remove();
			return;
		}

		for (ProgramInstance transaction : transactions) {
			if (transaction == null || transaction.getOperations() == null) {
				aborted.incrementAndGet();
				continue;
			}
			
			long startMs = System.currentTimeMillis();
			try {
				boolean success = executeTransaction(transaction);
				if (success) {
					committed.incrementAndGet();
					latencies.add(System.currentTimeMillis() - startMs);
				} else {
					aborted.incrementAndGet();
				}
			} catch (Exception e) {
				aborted.incrementAndGet();
			}
		}

		// Clear ThreadLocal
		threadDataCenterMap.remove();
	}

	private IsolationLevel resolveIsolationLevel(IsolationLevel declaredLevel) {
		IsolationLevel base = declaredLevel == null ? IsolationLevel.SNAPSHOT_ISOLATION : declaredLevel;
		switch (strategy) {
			case ALL_SER:
				return IsolationLevel.SERIALIZABLE;
			case NON_SER_AS_SI:
				return base == IsolationLevel.SERIALIZABLE ? IsolationLevel.SERIALIZABLE : IsolationLevel.SNAPSHOT_ISOLATION;
			case CURRENT:
			default:
				return base;
		}
	}

	private boolean executeTransaction(ProgramInstance txn) throws InterruptedException {
		// Get the data center assigned to the current thread
		Integer dcId = threadDataCenterMap.get();
		if (dcId == null) {
			return false;
		}

		DataCenterNode dataCenter = dataCenters.get(dcId);
		IsolationLevel effectiveLevel = resolveIsolationLevel(txn.getIsolationLevel());
		long txId = txnIdGenerator.incrementAndGet();
		long sts;
		synchronized (godLock) {
			sts = logicalClock.incrementAndGet();
		}

		// Transaction buffer
		Map<String, Integer> buffer = new HashMap<>();
		Set<String> readSet = new HashSet<>();
		Set<String> writeSet = new HashSet<>();

		// Execute operations
		for (StaticOperation op : txn.getOperations()) {
			String key = op.getKey();
			if (op.isWriteOp()) {
				writeSet.add(key);
				buffer.put(key, syntheticValue(key, logicalClock.get()));
			} else {
				readSet.add(key);
				// Internal read: check buffer first
				Integer bufferedValue = readInt(buffer, key);
				if (bufferedValue == null) {
					// External read: fetch the latest version with timestamp < sts
					readExt(dataCenter, key, sts);
				}
			}
		}

		long commitTs;
		if (needsConflictCheck(effectiveLevel, readSet, writeSet)) {
			// Conflict check set is non-empty; request conflict checks from other nodes (1 RTT)
			commitTs = requestConflictCheckFromPeers(txId, dcId, effectiveLevel, sts, readSet, writeSet);
		} else {
			// Conflict check set is empty; commit locally directly
			commitTs = directLocalCommit(txId, dcId, sts, readSet, writeSet);
		}
		if (commitTs < 0L) {
			return false;
		}

		// Apply writes locally immediately
		dataCenter.applyWrites(new HashMap<>(buffer), commitTs);

		// Asynchronously propagate updates to other replicas
		propagateToOtherReplicasAsync(dcId, txId, buffer, commitTs);

		// Wait for 1 RTT so updates can reach other nodes
		sleepMillis(randomRttMs());

		return true;
	}

private boolean needsConflictCheck(IsolationLevel level,
									   Set<String> readSet,
									   Set<String> writeSet) {
		if (level == IsolationLevel.SNAPSHOT_ISOLATION) {
			return !writeSet.isEmpty();
		}
		if (level == IsolationLevel.SERIALIZABLE) {
			return !(readSet.isEmpty() && writeSet.isEmpty());
		}
		return false;
	}

	private long directLocalCommit(long txId,
							int originDcId,
							long sts,
							Set<String> readSet,
							Set<String> writeSet) {
		synchronized (godLock) {
			long cts = logicalClock.incrementAndGet();
			commitLog.add(new CommitRecord(txId, sts, cts, readSet, writeSet, originDcId));
			committedTransactions.add(txId);
			commitTimestampByTxn.put(txId, cts);
			replicaVisibleTransactions.computeIfAbsent(originDcId, k -> ConcurrentHashMap.newKeySet()).add(txId);
			return cts;
		}
	}

	private long requestConflictCheckFromPeers(long txId,
									   int originDcId,
									   IsolationLevel level,
									   long sts,
									   Set<String> readSet,
									   Set<String> writeSet) throws InterruptedException {
		// Request -> other nodes
		sleepMillis(randomRttMs());

		// Collect conflict check results from all other nodes
		boolean conflictDetected = false;
		for (DataCenterNode dc : dataCenters) {
			if (dc.getId() == originDcId) {
				continue;
			}
			if (checkConflictLocallyAt(dc, level, sts, readSet, writeSet)) {
				conflictDetected = true;
				break;
			}
		}

		// Reply <- other nodes
		sleepMillis(randomRttMs());

		if (conflictDetected) {
			return -1L; // abort
		}

		// No conflict; commit locally at the origin node
		synchronized (godLock) {
			long cts = logicalClock.incrementAndGet();
			commitLog.add(new CommitRecord(txId, sts, cts, readSet, writeSet, originDcId));
			committedTransactions.add(txId);
			commitTimestampByTxn.put(txId, cts);
			replicaVisibleTransactions.computeIfAbsent(originDcId, k -> ConcurrentHashMap.newKeySet()).add(txId);
			return cts;
		}
	}

	private boolean checkConflictLocallyAt(DataCenterNode dc,
									  IsolationLevel level,
									  long sts,
									  Set<String> readSet,
									  Set<String> writeSet) {
		synchronized (godLock) {
			// Check all transactions committed after sts
			for (CommitRecord record : commitLog) {
				if (record.getCts() <= sts) {
					continue;
				}

				boolean wwConflict = hasIntersection(writeSet, record.getWriteSet());
				if (level == IsolationLevel.SNAPSHOT_ISOLATION) {
					if (wwConflict) {
						return true;
					}
					continue;
				}

				boolean rwConflict = hasIntersection(readSet, record.getWriteSet());
				boolean wrConflict = hasIntersection(writeSet, record.getReadSet());
				if (wwConflict || rwConflict || wrConflict) {
					return true;
				}
			}
			return false;
		}
	}

	// Read the specified key from the buffer
	private Integer readInt(Map<String, Integer> buffer, String key) {
		return buffer.get(key);
	}

	// Read the latest version with timestamp < sts from storage
	private int readExt(DataCenterNode dataCenter, String key, long sts) {
		long visibleTs = Math.max(0L, sts - 1L);
		return dataCenter.readVisibleValue(key, visibleTs);
	}

	private boolean checkTransVis(long txId, Set<Long> visibleToT) {
		for (Long visibleTxn : visibleToT) {
			if (!committedTransactions.contains(visibleTxn)) {
				return false;
			}
		}
		return true;
	}

	private boolean checkPrefix(long txId, Set<Long> visibleToT) {
		List<CommitRecord> records = new ArrayList<>(commitLog);
		for (int i = 0; i < records.size(); i++) {
			CommitRecord rec1 = records.get(i);
			for (int j = i + 1; j < records.size(); j++) {
				CommitRecord rec2 = records.get(j);
				if (rec1.getCts() < rec2.getCts() && visibleToT.contains(rec2.getTxId()) && 
				    !visibleToT.contains(rec1.getTxId())) {
					return false;
				}
			}
		}
		return true;
	}

	private boolean checkNoConflict(Set<Long> visibleToT, long cts, Set<String> readSet, Set<String> writeSet) {
		for (CommitRecord record : commitLog) {
			if (!visibleToT.contains(record.getTxId())) {
				// Check write-write conflicts
				boolean wwConflict = record.getWriteSet().stream()
						.anyMatch(writeSet::contains);
				if (wwConflict) {
					return false;
				}
			}
		}
		return true;
	}

	private boolean checkConflictByLevel(IsolationLevel level,
											 long sts,
											 long cts,
											 Set<String> readSet,
											 Set<String> writeSet) {
		if (level != IsolationLevel.SNAPSHOT_ISOLATION && level != IsolationLevel.SERIALIZABLE) {
			return true;
		}

		for (CommitRecord record : commitLog) {
			long otherCts = record.getCts();
			if (otherCts <= sts || otherCts >= cts) {
				continue;
			}

			boolean wwConflict = hasIntersection(writeSet, record.getWriteSet());
			if (level == IsolationLevel.SNAPSHOT_ISOLATION) {
				if (wwConflict) {
					return false;
				}
				continue;
			}

			boolean rwConflict = hasIntersection(readSet, record.getWriteSet());
			boolean wrConflict = hasIntersection(writeSet, record.getReadSet());
			if (wwConflict || rwConflict || wrConflict) {
				return false;
			}
		}

		return true;
	}

	private boolean hasIntersection(Set<String> a, Set<String> b) {
		for (String key : a) {
			if (b.contains(key)) {
				return true;
			}
		}
		return false;
	}

	private boolean checkTotalVis(Set<Long> visibleToT, long cts) {
		for (CommitRecord record : commitLog) {
			if (record.getCts() < cts && !visibleToT.contains(record.getTxId())) {
				return false;
			}
		}
		return true;
	}

	private void propagateToOtherReplicasAsync(int originDcId, long txId, Map<String, Integer> writes, long commitTs) {
		if (writes == null || writes.isEmpty()) {
			return;
		}

		// Asynchronously propagate to other replicas
		for (DataCenterNode targetDc : dataCenters) {
			if (targetDc.getId() == originDcId) {
				continue;
			}
			targetDc.getExecutor().submit(() -> {
				sleepMillis(randomRttMs());
				targetDc.applyWrites(new HashMap<>(writes), commitTs);
				synchronized (godLock) {
					replicaVisibleTransactions.computeIfAbsent(targetDc.getId(), k -> ConcurrentHashMap.newKeySet()).add(txId);
				}
			});
		}
	}

	private int syntheticValue(String key, long ts) {
		return Objects.hash(key, ts);
	}

	private int randomRttMs() {
		if (maxRttMs <= minRttMs) {
			return minRttMs;
		}
		return minRttMs + rttRandom.nextInt(maxRttMs - minRttMs + 1);
	}

	private static void sleepMillis(int millis) {
		if (millis <= 0) {
			return;
		}
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted during simulated WAN delay", e);
		}
	}

	private static double percentile(List<Long> values, double p) {
		if (values.isEmpty()) {
			return 0.0;
		}
		List<Long> sorted = new ArrayList<>(values);
		sorted.sort(Comparator.naturalOrder());
		double rank = (p / 100.0) * (sorted.size() - 1);
		int low = (int) Math.floor(rank);
		int high = (int) Math.ceil(rank);
		if (low == high) {
			return sorted.get(low);
		}
		double fraction = rank - low;
		return sorted.get(low) + fraction * (sorted.get(high) - sorted.get(low));
	}

	private static List<WorkloadSession> extractSessions(TemplateSet templateSet, String fileName) {
		List<WorkloadSession> sessions = templateSet.getSessions();
		if (sessions != null && !sessions.isEmpty()) {
			List<WorkloadSession> mapped = new ArrayList<>();
			for (WorkloadSession session : sessions) {
				List<ProgramInstance> txns = session.getInstances() == null ? Collections.emptyList() : session.getInstances();
				mapped.add(new WorkloadSession(session.getId(), txns));
			}
			return mapped;
		}

		List<ProgramInstance> templates = templateSet.getTemplates();
		if (templates == null || templates.isEmpty()) {
			return Collections.emptyList();
		}

		int inferredSessions = inferSessionsFromFilename(fileName);
		if (inferredSessions <= 0) {
			return Collections.singletonList(new WorkloadSession(1, templates));
		}

		List<WorkloadSession> result = new ArrayList<>();
		int txnsPerSession = Math.max(1, templates.size() / inferredSessions);
		for (int i = 0; i < inferredSessions; i++) {
			int start = i * txnsPerSession;
			int end = (i == inferredSessions - 1) ? templates.size() : Math.min(templates.size(), start + txnsPerSession);
			if (start >= templates.size()) {
				break;
			}
			result.add(new WorkloadSession(i + 1, templates.subList(start, end)));
		}
		return result;
	}

	private static int inferSessionsFromFilename(String fileName) {
		Pattern p = Pattern.compile("^[^_]+_(\\d+)s_(\\d+)t_.*\\.json$");
		Matcher m = p.matcher(fileName);
		if (m.matches()) {
			return Integer.parseInt(m.group(1));
		}
		return 0;
	}

	private void shutdown() {
		for (DataCenterNode dataCenter : dataCenters) {
			dataCenter.shutdown();
		}
	}
}
