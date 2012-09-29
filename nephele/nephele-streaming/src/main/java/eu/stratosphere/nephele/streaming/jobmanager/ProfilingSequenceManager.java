package eu.stratosphere.nephele.streaming.jobmanager;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.VertexAssignmentListener;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.StreamingPluginLoader;
import eu.stratosphere.nephele.streaming.actions.ActAsProfilingMasterAction;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingEdge;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingGroupVertex;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingSequence;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingVertex;
import eu.stratosphere.nephele.streaming.types.StreamProfilingReporterInfo;
import eu.stratosphere.nephele.util.StringUtils;

public class ProfilingSequenceManager implements VertexAssignmentListener {

	private static final Log LOG = LogFactory.getLog(ProfilingSequenceManager.class);

	private final ProfilingSequence profilingSequence;

	private HashMap<ExecutionVertexID, ProfilingVertex> verticesByID;

	private ExecutionGraph executionGraph;

	private int verticesWithPendingAllocation;

	private ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance> instances;

	private ConcurrentHashMap<InstanceConnectionInfo, StreamProfilingReporterInfo> profilingReporterInfos;

	public ProfilingSequenceManager(ProfilingSequence profilingSequence,
			ExecutionGraph executionGraph,
			ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance> instances) {

		this.profilingSequence = profilingSequence;
		this.executionGraph = executionGraph;
		this.instances = instances;
		this.profilingReporterInfos = new ConcurrentHashMap<InstanceConnectionInfo, StreamProfilingReporterInfo>();
		init();
	}

	private void init() {
		this.verticesByID = new HashMap<ExecutionVertexID, ProfilingVertex>();
		for (ProfilingGroupVertex groupVertex : profilingSequence.getSequenceVertices()) {
			for (ProfilingVertex vertex : groupVertex.getGroupMembers()) {
				verticesByID.put(vertex.getID(), vertex);
				if (vertex.getProfilingReporter() == null) {
					verticesWithPendingAllocation++;
				}
			}
		}
	}

	public void attachListenersToExecutionGraph() {
		for (ExecutionVertexID vertexID : verticesByID.keySet()) {
			ExecutionVertex vertex = executionGraph.getVertexByID(vertexID);
			vertex.registerVertexAssignmentListener(this);
		}
	}

	public void detachListenersFromExecutionGraph() {
		for (ExecutionVertexID vertexID : verticesByID.keySet()) {
			ExecutionVertex vertex = executionGraph.getVertexByID(vertexID);
			vertex.unregisterVertexAssignmentListener(this);
			vertex.removePluginData(StreamingPluginLoader.STREAMING_PLUGIN_ID);
		}
	}

	public ProfilingSequence getProfilingSequence() {
		return profilingSequence;
	}

	@Override
	public synchronized void vertexAssignmentChanged(ExecutionVertexID id, AllocatedResource newAllocatedResource) {
		ProfilingVertex vertex = verticesByID.get(id);
		if (vertex.getProfilingReporter() == null) {
			verticesWithPendingAllocation--;
		}
		AbstractInstance instance = newAllocatedResource.getInstance();
		instances.putIfAbsent(instance.getInstanceConnectionInfo(), instance);
		vertex.setProfilingReporter(instance.getInstanceConnectionInfo());
		if (verticesWithPendingAllocation == 0) {
			try {
				setupProfilingMasters();
				setupProfilingReporters();
			} catch (Exception e) {
				LOG.error(StringUtils.stringifyException(e));
			}

			// clear large memory structures
			profilingReporterInfos = null;
			verticesByID = null;
		}
	}

	private void setupProfilingReporters() throws Exception {
		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		final AtomicReference<Exception> exceptionCollector = new AtomicReference<Exception>();

		for (final StreamProfilingReporterInfo reporterInfo : this.profilingReporterInfos.values()) {
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						setupProfilingReporter(reporterInfo);
					} catch (Exception e) {
						LOG.error(StringUtils.stringifyException(e));
						exceptionCollector.set(e);
					}
				}
			});
		}
		threadPool.shutdown();

		try {
			threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
			if (exceptionCollector.get() != null) {
				throw exceptionCollector.get();
			}
			LOG.info("Successfully set up profiling reporters for " + profilingSequence.toString());
		} catch (InterruptedException e) {
			LOG.info("Interrupted while setting up profiling reporters for " + profilingSequence.toString());
			threadPool.shutdownNow();
		}
	}

	private void setupProfilingReporter(StreamProfilingReporterInfo reporterInfo) throws IOException {
		int attempts = 0;
		boolean success = false;
		while (!success) {
			attempts++;
			AbstractInstance instance = instances.get(reporterInfo.getReporterConnectionInfo());
			try {
				instance.sendData(StreamingPluginLoader.STREAMING_PLUGIN_ID, reporterInfo);
				success = true;
			} catch (IOException e) {
				if (attempts > 10) {
					LOG.error(String.format(
						"Received 10 IOExceptions when trying to setup profiling reporter on %s. Giving up",
						reporterInfo.getReporterConnectionInfo().toString()));
					throw e;
				}
			}
		}
		LOG.info("Successfully set up profiling reporter on " + reporterInfo.getReporterConnectionInfo());
	}

	private void setupProfilingMasters() throws Exception {
		final ProfilingGroupVertex anchor = determineProfilingAnchor();

		final HashMap<InstanceConnectionInfo, LinkedList<ProfilingVertex>> verticesByProfilingMaster = new HashMap<InstanceConnectionInfo, LinkedList<ProfilingVertex>>();

		for (ProfilingVertex vertex : anchor.getGroupMembers()) {
			InstanceConnectionInfo profilingMaster = vertex.getProfilingReporter();
			LinkedList<ProfilingVertex> verticesOnProfilingMaster = verticesByProfilingMaster.get(profilingMaster);
			if (verticesOnProfilingMaster == null) {
				verticesOnProfilingMaster = new LinkedList<ProfilingVertex>();
				verticesByProfilingMaster.put(profilingMaster, verticesOnProfilingMaster);
			}
			verticesOnProfilingMaster.add(vertex);
		}

		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		final AtomicReference<Exception> exceptionCollector = new AtomicReference<Exception>();

		for (final LinkedList<ProfilingVertex> profilingMasterVertices : verticesByProfilingMaster.values()) {
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						setupProfilingMaster(anchor, profilingMasterVertices);
					} catch (Exception e) {
						LOG.error(StringUtils.stringifyException(e));
						exceptionCollector.set(e);
					}
				}
			});
		}
		threadPool.shutdown();

		try {
			threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
			if (exceptionCollector.get() != null) {
				throw exceptionCollector.get();
			}
			LOG.info("Successfully set up profiling masters for " + profilingSequence.toString());
		} catch (InterruptedException e) {
			LOG.info("Interrupted while setting up profiling masters for " + profilingSequence.toString());
			threadPool.shutdownNow();
		}
	}

	private void setupProfilingMaster(ProfilingGroupVertex anchor,
			LinkedList<ProfilingVertex> anchorVerticesOnProfilingMaster) throws IOException {

		InstanceConnectionInfo profilingMaster = anchorVerticesOnProfilingMaster.getFirst().getProfilingReporter();
		ProfilingSequence partialSequence = expandToPartialProfilingSequence(anchor,
			anchorVerticesOnProfilingMaster);
		registerProfilingMasterOnExecutionVertices(partialSequence, profilingMaster);
		sendPartialProfilingSequenceToProfilingMaster(profilingMaster, partialSequence);
		LOG.info("Successfully set up profiling master " + profilingMaster);
	}

	private void registerProfilingMasterOnExecutionVertices(ProfilingSequence partialSequence,
			InstanceConnectionInfo profilingMaster) {

		List<ProfilingGroupVertex> groupVertices = partialSequence.getSequenceVertices();
		for (int i = 0; i < groupVertices.size(); i++) {

			boolean isStartVertex = (i == 0);
			boolean isEndVertex = (i == (groupVertices.size() - 1));
			boolean includeVertexInProfiling = true;
			if ((isStartVertex && !partialSequence.isIncludeStartVertex())
					|| (isEndVertex && !partialSequence.isIncludeEndVertex())) {
				includeVertexInProfiling = false;
			}

			for (ProfilingVertex vertex : groupVertices.get(i).getGroupMembers()) {
				StreamProfilingReporterInfo tmProfilingInfo = getProfilingReporterInfo(vertex.getProfilingReporter());

				if (includeVertexInProfiling) {
					tmProfilingInfo.addTaskProfilingMaster(vertex.getID(), profilingMaster);
				}

				// register for channel throughput and output buffer lifetimes (measured at output channel)
				if (!isEndVertex) {
					for (ProfilingEdge forwardEdge : vertex.getForwardEdges()) {
						tmProfilingInfo.addChannelProfilingMaster(forwardEdge.getSourceChannelID(), profilingMaster);
					}
				}

				// register for channel latencies (measured an input channel)
				if (!isStartVertex) {
					for (ProfilingEdge backwardEdge : vertex.getBackwardEdges()) {
						tmProfilingInfo.addChannelProfilingMaster(backwardEdge.getSourceChannelID(), profilingMaster);
					}
				}
			}
		}
	}

	private StreamProfilingReporterInfo getProfilingReporterInfo(InstanceConnectionInfo reporter) {
		StreamProfilingReporterInfo profilingInfo = profilingReporterInfos.get(reporter);
		if (profilingInfo == null) {
			profilingInfo = new StreamProfilingReporterInfo(executionGraph.getJobID(), reporter);
			StreamProfilingReporterInfo previous = this.profilingReporterInfos.putIfAbsent(reporter, profilingInfo);
			if (previous != null) {
				profilingInfo = previous;
			}
		}
		return profilingInfo;
	}

	private ProfilingSequence expandToPartialProfilingSequence(ProfilingGroupVertex anchor,
			LinkedList<ProfilingVertex> anchorMembersToExpand) {

		ProfilingSequence partialProfilingSequence = profilingSequence.cloneWithoutGroupMembers();
		ProfilingGroupVertex clonedAnchor = getClonedAnchor(partialProfilingSequence, anchor);

		HashMap<ExecutionVertexID, ProfilingVertex> alreadyClonedVertices = new HashMap<ExecutionVertexID, ProfilingVertex>();
		for (ProfilingVertex anchorMemberToClone : anchorMembersToExpand) {
			cloneForward(clonedAnchor, anchorMemberToClone, alreadyClonedVertices);
			cloneBackward(clonedAnchor, anchorMemberToClone, alreadyClonedVertices);
		}

		return partialProfilingSequence;
	}

	private ProfilingVertex cloneBackward(ProfilingGroupVertex groupVertex, ProfilingVertex toClone,
			HashMap<ExecutionVertexID, ProfilingVertex> alreadyClonedVertices) {

		ProfilingVertex cloned = alreadyClonedVertices.get(toClone.getID());
		if (cloned == null) {
			cloned = new ProfilingVertex(toClone.getID(), toClone.getName());
			cloned.setProfilingReporter(toClone.getProfilingReporter());
			alreadyClonedVertices.put(toClone.getID(), cloned);
			groupVertex.addGroupMember(cloned);
		}

		for (ProfilingEdge edgeToClone : toClone.getBackwardEdges()) {
			ProfilingEdge clonedEdge = new ProfilingEdge(edgeToClone.getSourceChannelID(),
				edgeToClone.getTargetChannelID());
			clonedEdge.setTargetVertex(cloned);
			clonedEdge.setTargetVertexEdgeIndex(cloned.getBackwardEdges().size());
			cloned.addBackwardEdge(clonedEdge);
			ProfilingVertex clonedSource = cloneBackward(groupVertex.getBackwardEdge().getSourceVertex(),
				edgeToClone.getSourceVertex(), alreadyClonedVertices);
			clonedEdge.setSourceVertex(clonedSource);
			clonedEdge.setSourceVertexEdgeIndex(clonedSource.getForwardEdges().size());
			clonedSource.addForwardEdge(clonedEdge);
		}

		return cloned;
	}

	private ProfilingGroupVertex getClonedAnchor(ProfilingSequence clonedSequence,
			ProfilingGroupVertex anchor) {

		for (ProfilingGroupVertex vertex : clonedSequence.getSequenceVertices()) {
			if (vertex.getJobVertexID().equals(anchor.getJobVertexID())) {
				return vertex;
			}
		}
		throw new RuntimeException("Could not find cloned anchor group vertex.");
	}

	private ProfilingVertex cloneForward(ProfilingGroupVertex groupVertex,
			ProfilingVertex toClone,
			HashMap<ExecutionVertexID, ProfilingVertex> alreadyClonedVertices) {

		ProfilingVertex cloned = alreadyClonedVertices.get(toClone.getID());
		if (cloned == null) {
			cloned = new ProfilingVertex(toClone.getID(), toClone.getName());
			cloned.setProfilingReporter(toClone.getProfilingReporter());
			alreadyClonedVertices.put(toClone.getID(), cloned);
			groupVertex.addGroupMember(cloned);
		}

		for (ProfilingEdge edgeToClone : toClone.getForwardEdges()) {
			ProfilingEdge clonedEdge = new ProfilingEdge(edgeToClone.getSourceChannelID(),
				edgeToClone.getTargetChannelID());
			clonedEdge.setSourceVertex(cloned);
			clonedEdge.setSourceVertexEdgeIndex(cloned.getForwardEdges().size());
			cloned.addForwardEdge(clonedEdge);
			ProfilingVertex clonedTarget = cloneForward(groupVertex.getForwardEdge().getTargetVertex(),
				edgeToClone.getTargetVertex(), alreadyClonedVertices);
			clonedEdge.setTargetVertex(clonedTarget);
			clonedEdge.setTargetVertexEdgeIndex(clonedTarget.getBackwardEdges().size());
			clonedTarget.addBackwardEdge(clonedEdge);
		}

		return cloned;
	}

	private void sendPartialProfilingSequenceToProfilingMaster(InstanceConnectionInfo profilingMaster,
			ProfilingSequence partialSequence) throws IOException {
		int attempts = 0;
		boolean success = false;
		while (!success) {
			attempts++;
			AbstractInstance instance = instances.get(profilingMaster);
			try {
				instance.sendData(StreamingPluginLoader.STREAMING_PLUGIN_ID,
					new ActAsProfilingMasterAction(executionGraph.getJobID(), partialSequence));
				success = true;
			} catch (IOException e) {
				if (attempts > 10) {
					LOG.error("Received 10 IOException when trying to contact task manager. Giving up.");
					throw e;
				}
			}
		}
	}

	private ProfilingGroupVertex determineProfilingAnchor() {
		// find anchoring GroupVertex G with highest instanceCount

		int anchorVertexInstanceCount = -1;
		ProfilingGroupVertex anchorVertex = null;

		for (ProfilingGroupVertex groupVertex : profilingSequence.getSequenceVertices()) {
			if (groupVertex.getNumberOfExecutingInstances() > anchorVertexInstanceCount) {
				anchorVertexInstanceCount = groupVertex.getNumberOfExecutingInstances();
				anchorVertex = groupVertex;
			}
		}

		return anchorVertex;
	}
}
