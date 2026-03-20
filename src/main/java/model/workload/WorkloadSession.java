package model.workload;

import java.util.List;

public class WorkloadSession {
	private int id;
	private List<ProgramInstance> instances;

	public WorkloadSession() {
		this.id = 0;
		this.instances = null;
	}

	public WorkloadSession(int id, List<ProgramInstance> instances) {
		this.id = id;
		this.instances = instances;
	}

	public int getId() {
		return id;
	}

	public List<ProgramInstance> getInstances() {
		return instances;
	}
}
