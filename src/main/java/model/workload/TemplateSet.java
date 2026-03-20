package model.workload;

import java.util.ArrayList;
import java.util.List;

// 添加Jackson ObjectMapper依赖


public class TemplateSet {
	private List<ProgramInstance> templates;
	private List<WorkloadSession> sessions;

	public TemplateSet(List<ProgramInstance> templates) {
		this.templates = templates;
		this.sessions = null;
	}

	public TemplateSet() {
		this.templates = null;
		this.sessions = null;
	}

	public TemplateSet(List<ProgramInstance> templates, List<WorkloadSession> sessions) {
		this.templates = templates;
		this.sessions = sessions;
	}

	public List<ProgramInstance> getTemplates() {
		if (templates != null) {
			return templates;
		}

		if (sessions != null) {
			List<ProgramInstance> flattened = new ArrayList<>();
			for (WorkloadSession session : sessions) {
				if (session.getInstances() != null) {
					flattened.addAll(session.getInstances());
				}
			}
			return flattened;
		}

		return templates;
	}

	public List<WorkloadSession> getSessions() {
		return sessions;
	}

	public String toString() {
		return super.toString();
	}
}