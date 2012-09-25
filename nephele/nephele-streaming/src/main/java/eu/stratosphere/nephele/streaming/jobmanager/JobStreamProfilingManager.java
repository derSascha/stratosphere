package eu.stratosphere.nephele.streaming.jobmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingGraphFactory;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingSequence;

public class JobStreamProfilingManager {

	private static final Log LOG = LogFactory.getLog(JobStreamProfilingManager.class);

	private ExecutionGraph executionGraph;

	private ArrayList<ProfilingSequenceManager> profilingSequenceManagers;

	private ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance> instances;

	public JobStreamProfilingManager(ExecutionGraph executionGraph) {
		this.executionGraph = executionGraph;
		this.instances = new ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance>();
		initProfilingSequenceManagers();
	}

	private void initProfilingSequenceManagers() {
		// FIXME naive implementation until we can annotate the job
		// subgraphStart and subgraphEnd should be derived from the annotations
		ExecutionGroupVertex startVertex = this.executionGraph.getInputVertex(0).getGroupVertex();
		ExecutionGroupVertex endVertex = this.executionGraph.getOutputVertex(0).getGroupVertex();

		List<ProfilingSequence> profilingSequences = ProfilingGraphFactory.createProfilingSequences(
			this.executionGraph, startVertex,
			false, endVertex, false);
		this.profilingSequenceManagers = new ArrayList<ProfilingSequenceManager>(profilingSequences.size());

		for (ProfilingSequence profilingSequence : profilingSequences) {
			LOG.info("Setting up profiling sequence " + profilingSequence.toString());
			ProfilingSequenceManager sequenceManager = new ProfilingSequenceManager(profilingSequence,
				this.executionGraph,
				this.instances);
			sequenceManager.attachListenersAndPluginDataToExecutionGraph();
			this.profilingSequenceManagers.add(sequenceManager);
		}
	}

	public void shutdown() {
		for (ProfilingSequenceManager sequenceManager : this.profilingSequenceManagers) {
			sequenceManager.detachListenersAndPluginDataFromExecutionGraph();
		}
		this.profilingSequenceManagers.clear();
		this.instances.clear();
		this.executionGraph = null;
	}
}
