package model.workload;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WorkloadMeta {
	private final String benchmark;
	private final int sessions;
	private final int txnsPerSession;
	private final int maxKey;
	private final int caseNum;
	private final int totalTxns;

	public WorkloadMeta(String benchmark, int sessions, int txnsPerSession, int maxKey, int caseNum, int totalTxns) {
		this.benchmark = benchmark;
		this.sessions = sessions;
		this.txnsPerSession = txnsPerSession;
		this.maxKey = maxKey;
		this.caseNum = caseNum;
		this.totalTxns = totalTxns;
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

	public static WorkloadMeta fromFilename(String fileName, List<WorkloadSession> sessions) {
		Pattern p = Pattern.compile("^([^_]+)_(\\d+)s_(\\d+)t_(\\d+)k_(\\d+)\\.json$");
		Matcher m = p.matcher(fileName);
		if (m.matches()) {
			String benchmark = m.group(1);
			int sessionCount = Integer.parseInt(m.group(2));
			int txnsPerSession = Integer.parseInt(m.group(3));
			int maxKey = Integer.parseInt(m.group(4));
			int caseNum = Integer.parseInt(m.group(5));
			int total = sessionCount * txnsPerSession;
			return new WorkloadMeta(benchmark, sessionCount, txnsPerSession, maxKey, caseNum, total);
		}

		int totalTxns = 0;
		for (WorkloadSession session : sessions) {
			List<ProgramInstance> instances = session.getInstances();
			if (instances != null) {
				totalTxns += instances.size();
			}
		}
		int inferredSessions = Math.max(1, sessions.size());
		int inferredTps = Math.max(1, totalTxns / inferredSessions);
		String benchmark = fileName.contains("_") ? fileName.substring(0, fileName.indexOf('_')) : fileName;
		return new WorkloadMeta(benchmark, inferredSessions, inferredTps, -1, -1, totalTxns);
	}
}