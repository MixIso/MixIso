package model.execution;

import model.core.IsolationLevel;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TxContext {
	private final IsolationLevel level;
	private final long txId;
	private final long sts;
	private final int originDcId;
	private final Set<Long> visibleTxnIds;
	private final Map<String, Integer> buffer;
	private int lastReadValue;

	public TxContext(IsolationLevel level, long txId, long sts, int originDcId, Set<Long> visibleTxnIds) {
		this.level = level;
		this.txId = txId;
		this.sts = sts;
		this.originDcId = originDcId;
		this.visibleTxnIds = new HashSet<>(visibleTxnIds);
		this.buffer = new HashMap<>();
		this.lastReadValue = 0;
	}

	public IsolationLevel getLevel() {
		return level;
	}

	public long getSts() {
		return sts;
	}

	public long getTxId() {
		return txId;
	}

	public int getOriginDcId() {
		return originDcId;
	}

	public Set<Long> getVisibleTxnIds() {
		return visibleTxnIds;
	}

	public Map<String, Integer> getBuffer() {
		return buffer;
	}

	public int getLastReadValue() {
		return lastReadValue;
	}

	public void setLastReadValue(int lastReadValue) {
		this.lastReadValue = lastReadValue;
	}
}