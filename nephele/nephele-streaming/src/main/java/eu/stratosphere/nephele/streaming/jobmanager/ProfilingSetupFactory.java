package eu.stratosphere.nephele.streaming.jobmanager;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;

public class ProfilingSetupFactory {

	private List<ProfilingSequenceManager> profilingSequenceManagers;

	private HashMap<ExecutionVertexID, Set<InstanceConnectionInfo>> collectedProfilingDataTargets;

	public ProfilingSetupFactory(List<ProfilingSequenceManager> profilingSequenceManagers) {
		this.profilingSequenceManagers = profilingSequenceManagers;
		this.collectedProfilingDataTargets = new HashMap<ExecutionVertexID, Set<InstanceConnectionInfo>>();
	}

	public void computeDistributedProfilingSetup() {
		for (ProfilingSequenceManager sequenceManager : profilingSequenceManagers) {
			
		}
	}
}
