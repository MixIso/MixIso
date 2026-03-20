package model.execution;

public class ExecutionResult {
	private final String fileName;
	private final String benchmark;
	private final int sessions;
	private final int txnsPerSession;
	private final int maxKey;
	private final int caseNum;
	private final int totalTxns;
	private final long committedTxns;
	private final long abortedTxns;
	private final double throughputTxPerSec;
	private final double avgLatencyMs;
	private final double p95LatencyMs;
	private final int dcCount;
	private final int minRttMs;
	private final int maxRttMs;

	public ExecutionResult(String fileName,
						 String benchmark,
						 int sessions,
						 int txnsPerSession,
						 int maxKey,
						 int caseNum,
						 int totalTxns,
						 long committedTxns,
						 long abortedTxns,
						 double throughputTxPerSec,
						 double avgLatencyMs,
						 double p95LatencyMs,
						 int dcCount,
						 int minRttMs,
						 int maxRttMs) {
		this.fileName = fileName;
		this.benchmark = benchmark;
		this.sessions = sessions;
		this.txnsPerSession = txnsPerSession;
		this.maxKey = maxKey;
		this.caseNum = caseNum;
		this.totalTxns = totalTxns;
		this.committedTxns = committedTxns;
		this.abortedTxns = abortedTxns;
		this.throughputTxPerSec = throughputTxPerSec;
		this.avgLatencyMs = avgLatencyMs;
		this.p95LatencyMs = p95LatencyMs;
		this.dcCount = dcCount;
		this.minRttMs = minRttMs;
		this.maxRttMs = maxRttMs;
	}

	public String getFileName() {
		return fileName;
	}

	public String getBenchmark() {
		return benchmark;
	}

	public int getSessions() {
		return sessions;
	}

	public int getTxnsPerSession() {
		return txnsPerSession;
	}

	public int getMaxKey() {
		return maxKey;
	}

	public int getCaseNum() {
		return caseNum;
	}

	public int getTotalTxns() {
		return totalTxns;
	}

	public long getCommittedTxns() {
		return committedTxns;
	}

	public long getAbortedTxns() {
		return abortedTxns;
	}

	public double getThroughputTxPerSec() {
		return throughputTxPerSec;
	}

	public double getAvgLatencyMs() {
		return avgLatencyMs;
	}

	public double getP95LatencyMs() {
		return p95LatencyMs;
	}

	public int getDcCount() {
		return dcCount;
	}

	public int getMinRttMs() {
		return minRttMs;
	}

	public int getMaxRttMs() {
		return maxRttMs;
	}
}