package model.execution;

public class Version {
	private final long ts;
	private final int value;

	public Version(long ts, int value) {
		this.ts = ts;
		this.value = value;
	}

	public long getTs() {
		return ts;
	}

	public int getValue() {
		return value;
	}
}