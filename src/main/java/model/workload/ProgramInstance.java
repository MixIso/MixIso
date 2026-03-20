package model.workload;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import model.core.IsolationLevel;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProgramInstance {
	private final String name;
	private final IsolationLevel isolationLevel;
	private final List<StaticOperation> operations;

	@JsonIgnore
	public boolean isSER() {
		return isolationLevel.equals(IsolationLevel.SERIALIZABLE);
	}

	@JsonIgnore
	public boolean isSI() {
		return isolationLevel.equals(IsolationLevel.SNAPSHOT_ISOLATION);
	}

	@JsonIgnore
	public boolean isPC() {
		return isolationLevel.equals(IsolationLevel.PREFIX_CONSISTENCY);
	}

	@JsonIgnore
	public boolean isPSI() {
		return isolationLevel.equals(IsolationLevel.PARALLEL_SNAPSHOT_ISOLATION);
	}

	@JsonIgnore
	public boolean isCC() {
		return isolationLevel.equals(IsolationLevel.CAUSAL_CONSISTENCY);
	}

	@JsonIgnore
	public boolean isRA() {
		return isolationLevel.equals(IsolationLevel.READ_ATOMIC);
	}


	@JsonIgnore
	public boolean isReadOnly() {
		return getWriteSet().isEmpty();
	}

	@JsonIgnore
	public boolean isWriteOnly() {
		return getReadSet().isEmpty();
	}

	@JsonIgnore
	public boolean isSingleRead() {
		return getWriteSet().isEmpty() && getReadSet().size() == 1;
	}

	public ProgramInstance() {
		this.name = null;
		this.isolationLevel = null;
		this.operations = null;
//		System.err.println("Transaction() called " + ++txnCount);
	}

	public ProgramInstance(String name, IsolationLevel isolationLevel, List<StaticOperation> operations) {
		this.name = name;
		this.isolationLevel = isolationLevel;
		this.operations = operations;
	}

	public IsolationLevel getIsolationLevel() {
		return isolationLevel;
	}

	public List<StaticOperation> getOperations() {
		return operations;
	}

	@JsonIgnore
	public boolean wwConflict(ProgramInstance txn) {
		for (StaticOperation op1 : operations) {
			for (StaticOperation op2 : txn.operations) {
				if (Objects.equals(op1.getKey(), op2.getKey()) && op1.isWriteOp() && op2.isWriteOp()) {
					return true;
				}
			}
		}
		return false;
	}

	@JsonIgnore
	public boolean wrConflict(ProgramInstance txn) {
		for (StaticOperation op1 : operations) {
			for (StaticOperation op2 : txn.operations) {
				if (Objects.equals(op1.getKey(), op2.getKey()) && op1.isWriteOp() && !op2.isWriteOp()) {
					return true;
				}
			}
		}
		return false;
	}

	public String getName() {
		return name;
	}

	@JsonIgnore
	public Set<String> getWriteSet() {
		Set<String> writeSet = new HashSet<>();
		if (operations != null) {
			for (StaticOperation op : operations) {
				if (op.isWriteOp()) {
					writeSet.add(op.getKey());
				}
			}
		}
		return writeSet;
	}

	@JsonIgnore
	public Set<String> getReadSet() {
		Set<String> readSet = new HashSet<>();
		if (operations != null) {
			for (StaticOperation op : operations) {
				if (op.isReadOp()) {
					readSet.add(op.getKey());
				}
			}
		}
		return readSet;
	}

	public String toString() {
		return "{\"templateName\": " + name + ", \"isolationLevel\": \"" + isolationLevel + "\", \"operations\": " + operations + "}";
	}
}