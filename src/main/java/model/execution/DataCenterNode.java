package model.execution;

import model.workload.ProgramInstance;
import model.workload.StaticOperation;
import model.workload.WorkloadSession;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataCenterNode {
	private final int id;
	private final ExecutorService executor;
	private final ConcurrentHashMap<String, CopyOnWriteArrayList<Version>> localStore;

	public DataCenterNode(int id) {
		this.id = id;
		this.executor = Executors.newSingleThreadExecutor();
		this.localStore = new ConcurrentHashMap<>();
	}

	public int getId() {
		return id;
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public void shutdown() {
		executor.shutdownNow();
	}

	public void seedInitialData(List<WorkloadSession> sessions) {
		for (WorkloadSession session : sessions) {
			List<ProgramInstance> instances = session.getInstances();
			if (instances == null) {
				continue;
			}
			for (ProgramInstance txn : instances) {
				if (txn == null || txn.getOperations() == null) {
					continue;
				}
				for (StaticOperation op : txn.getOperations()) {
					localStore.computeIfAbsent(op.getKey(), k -> {
						CopyOnWriteArrayList<Version> versions = new CopyOnWriteArrayList<>();
						versions.add(new Version(0L, 0));
						return versions;
					});
				}
			}
		}
	}

	public int readVisibleValue(String key, long visibleTs) {
		CopyOnWriteArrayList<Version> versions = localStore.computeIfAbsent(key, k -> {
			CopyOnWriteArrayList<Version> init = new CopyOnWriteArrayList<>();
			init.add(new Version(0L, 0));
			return init;
		});

		Version best = versions.get(0);
		for (Version version : versions) {
			if (version.getTs() <= visibleTs && version.getTs() >= best.getTs()) {
				best = version;
			}
		}
		return best.getValue();
	}

	public void applyWrites(Map<String, Integer> writes, long commitTs) {
		for (Map.Entry<String, Integer> entry : writes.entrySet()) {
			CopyOnWriteArrayList<Version> versions = localStore.computeIfAbsent(entry.getKey(), k -> {
				CopyOnWriteArrayList<Version> init = new CopyOnWriteArrayList<>();
				init.add(new Version(0L, 0));
				return init;
			});
			versions.add(new Version(commitTs, entry.getValue()));
		}
	}
}