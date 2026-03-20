package model.execution;

import java.util.HashSet;
import java.util.Set;

public class CommitRecord {
	private final long txId;
	private final long sts;
	private final long cts;
	private final int originDcId;
	private final Set<String> readSet;
	private final Set<String> writeSet;

	public CommitRecord(long txId, long sts, long cts, Set<String> readSet, Set<String> writeSet, int originDcId) {
		this.txId = txId;
		this.sts = sts;
		this.cts = cts;
		this.originDcId = originDcId;
		this.readSet = new HashSet<>(readSet);
		this.writeSet = new HashSet<>(writeSet);
	}

	public long getTxId() {
		return txId;
	}

	public long getSts() {
		return sts;
	}

	public long getCts() {
		return cts;
	}

	public int getOriginDcId() {
		return originDcId;
	}

	public Set<String> getReadSet() {
		return readSet;
	}

	public Set<String> getWriteSet() {
		return writeSet;
	}
}