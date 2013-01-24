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
import eu.stratosphere.nephele.streaming.message.action.ActAsQosManagerAction;
import eu.stratosphere.nephele.streaming.message.action.ActAsQosReporterAction;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingSequence;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingVertex;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class is used on the job manager to compute a QoS setup for a given
 * {@link ProfilingSequence}. To do so, it attaches itself as an
 * {@link VertexAssignmentListener} to the respective exection vertices and as
 * soon as there are no more pending instance assignments, it will compute the
 * QoS setup (which QoS managers and reportes reside on which task manager) and
 * distributes this information to the task managers via RPC.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class ProfilingSequenceManager implements VertexAssignmentListener {

	private static final Log LOG = LogFactory
			.getLog(ProfilingSequenceManager.class);

	private final ProfilingSequence profilingSequence;

	private HashMap<ExecutionVertexID, ProfilingVertex> verticesByID;

	private ExecutionGraph executionGraph;

	private int verticesWithPendingAllocation;

	private ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance> instances;

	private ConcurrentHashMap<InstanceConnectionInfo, ActAsQosReporterAction> qosReporterInfos;

	public ProfilingSequenceManager(
			ProfilingSequence profilingSequence,
			ExecutionGraph executionGraph,
			ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance> instances) {

		this.profilingSequence = profilingSequence;
		this.executionGraph = executionGraph;
		this.instances = instances;
		this.qosReporterInfos = new ConcurrentHashMap<InstanceConnectionInfo, ActAsQosReporterAction>();
		this.init();
	}

	private void init() {
		this.verticesByID = new HashMap<ExecutionVertexID, ProfilingVertex>();
		for (ProfilingGroupVertex groupVertex : this.profilingSequence
				.getSequenceVertices()) {
			for (ProfilingVertex vertex : groupVertex.getGroupMembers()) {
				this.verticesByID.put(vertex.getID(), vertex);
				if (vertex.getQosReporter() == null) {
					this.verticesWithPendingAllocation++;
				}
			}
		}
	}

	public void attachListenersToExecutionGraph() {
		for (ExecutionVertexID vertexID : this.verticesByID.keySet()) {
			ExecutionVertex vertex = this.executionGraph
					.getVertexByID(vertexID);
			vertex.registerVertexAssignmentListener(this);
		}
	}

	public void detachListenersFromExecutionGraph() {
		for (ExecutionVertexID vertexID : this.verticesByID.keySet()) {
			ExecutionVertex vertex = this.executionGraph
					.getVertexByID(vertexID);
			vertex.unregisterVertexAssignmentListener(this);
			vertex.removePluginData(StreamingPluginLoader.STREAMING_PLUGIN_ID);
		}
	}

	public ProfilingSequence getProfilingSequence() {
		return this.profilingSequence;
	}

	@Override
	public synchronized void vertexAssignmentChanged(ExecutionVertexID id,
			AllocatedResource newAllocatedResource) {
		ProfilingVertex vertex = this.verticesByID.get(id);
		if (vertex.getQosReporter() == null) {
			this.verticesWithPendingAllocation--;
		}
		AbstractInstance instance = newAllocatedResource.getInstance();
		this.instances.putIfAbsent(instance.getInstanceConnectionInfo(),
				instance);
		vertex.setQosReporter(instance.getInstanceConnectionInfo());
		if (this.verticesWithPendingAllocation == 0) {
			try {
				this.setupQosManagers();
				this.setupQosReporters();
			} catch (Exception e) {
				LOG.error(StringUtils.stringifyException(e));
			}

			// clear large memory structures
			this.qosReporterInfos = null;
			this.verticesByID = null;
		}
	}

	private void setupQosReporters() throws Exception {
		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		final AtomicReference<Exception> exceptionCollector = new AtomicReference<Exception>();

		for (final ActAsQosReporterAction reporterInfo : this.qosReporterInfos.values()) {
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						ProfilingSequenceManager.this
								.setupQosReporter(reporterInfo);
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
			LOG.info("Successfully set up QoS reporters for "
					+ this.profilingSequence.toString());
		} catch (InterruptedException e) {
			LOG.info("Interrupted while setting up QoS reporters for "
					+ this.profilingSequence.toString());
			threadPool.shutdownNow();
		}
	}

	private void setupQosReporter(ActAsQosReporterAction reporterInfo)
			throws IOException {
		int attempts = 0;
		boolean success = false;
		while (!success) {
			attempts++;
			AbstractInstance instance = this.instances.get(reporterInfo
					.getReporterConnectionInfo());
			try {
				instance.sendData(StreamingPluginLoader.STREAMING_PLUGIN_ID,
						reporterInfo);
				success = true;
			} catch (IOException e) {
				if (attempts > 10) {
					LOG.error(String
							.format("Received 10 IOExceptions when trying to setup profiling reporter on %s. Giving up",
									reporterInfo.getReporterConnectionInfo()
											.toString()));
					throw e;
				}
			}
		}
		LOG.info("Successfully set up profiling reporter on "
				+ reporterInfo.getReporterConnectionInfo());
	}

	private void setupQosManagers() throws Exception {
		final ProfilingGroupVertex anchor = this.determineProfilingAnchor();

		final HashMap<InstanceConnectionInfo, LinkedList<ProfilingVertex>> verticesByQosManager = new HashMap<InstanceConnectionInfo, LinkedList<ProfilingVertex>>();

		for (ProfilingVertex vertex : anchor.getGroupMembers()) {
			InstanceConnectionInfo qosManager = vertex
					.getQosReporter();
			LinkedList<ProfilingVertex> verticesOnQosManager = verticesByQosManager
					.get(qosManager);
			if (verticesOnQosManager == null) {
				verticesOnQosManager = new LinkedList<ProfilingVertex>();
				verticesByQosManager.put(qosManager,
						verticesOnQosManager);
			}
			verticesOnQosManager.add(vertex);
		}

		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		final AtomicReference<Exception> exceptionCollector = new AtomicReference<Exception>();

		for (final LinkedList<ProfilingVertex> qosManagerVertices : verticesByQosManager
				.values()) {
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						ProfilingSequenceManager.this.setupQosManager(
								anchor, qosManagerVertices);
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
			LOG.info("Successfully set up profiling masters for "
					+ this.profilingSequence.toString());
		} catch (InterruptedException e) {
			LOG.info("Interrupted while setting up profiling masters for "
					+ this.profilingSequence.toString());
			threadPool.shutdownNow();
		}
	}

	private void setupQosManager(ProfilingGroupVertex anchor,
			LinkedList<ProfilingVertex> anchorVerticesOnQosManager)
			throws IOException {

		InstanceConnectionInfo qosManager = anchorVerticesOnQosManager
				.getFirst().getQosReporter();
		ProfilingSequence partialSequence = this
				.expandToPartialProfilingSequence(anchor,
						anchorVerticesOnQosManager);
		partialSequence.setQosManager(qosManager);

		this.registerQosManagerOnExecutionVertices(partialSequence,
				qosManager);
		this.sendPartialProfilingSequenceToQosManager(qosManager,
				partialSequence);
		LOG.info("Successfully set up profiling master " + qosManager);
	}

	private void registerQosManagerOnExecutionVertices(
			ProfilingSequence partialSequence,
			InstanceConnectionInfo qosManager) {

		List<ProfilingGroupVertex> groupVertices = partialSequence
				.getSequenceVertices();
		for (int i = 0; i < groupVertices.size(); i++) {

			boolean isStartVertex = i == 0;
			boolean isEndVertex = i == groupVertices.size() - 1;
			boolean includeVertexInProfiling = (!isStartVertex || partialSequence.isIncludeStartVertex())
					&& (!isEndVertex || partialSequence.isIncludeEndVertex());
			
			for (ProfilingVertex vertex : groupVertices.get(i).getGroupMembers()) {
				ActAsQosReporterAction tmProfilingInfo = this
						.getQosReporterAction(vertex.getQosReporter());

				if (includeVertexInProfiling) {
					// FIXME: add info InputGate -> OutputGate to profiling
					// as this may not be unique in case of multiple input- or
					// output gates
					tmProfilingInfo.addTaskQosManager(vertex.getID(),
							qosManager);
				}

				// register for throughput, output buffer lifetime, records per second
				// and records per buffer measurements (measured at output channel)
				if (!isEndVertex) {
					for (ProfilingEdge forwardEdge : vertex.getForwardEdges()) {
						tmProfilingInfo.addChannelQosManager(
								forwardEdge.getSourceChannelID(),
								qosManager);
					}
				}

				// register for channel latencies and channel throughput 
				// (measured an input channel)
				if (!isStartVertex) {
					for (ProfilingEdge backwardEdge : vertex.getBackwardEdges()) {
						tmProfilingInfo.addChannelQosManager(
								backwardEdge.getSourceChannelID(),
								qosManager);
					}
				}
			}
		}
	}

	private ActAsQosReporterAction getQosReporterAction(
			InstanceConnectionInfo reporter) {
		ActAsQosReporterAction profilingInfo = this.qosReporterInfos
				.get(reporter);
		if (profilingInfo == null) {
			profilingInfo = new ActAsQosReporterAction(
					this.executionGraph.getJobID(), reporter);
			ActAsQosReporterAction previous = this.qosReporterInfos
					.putIfAbsent(reporter, profilingInfo);
			if (previous != null) {
				profilingInfo = previous;
			}
		}
		return profilingInfo;
	}

	private ProfilingSequence expandToPartialProfilingSequence(
			ProfilingGroupVertex anchor,
			LinkedList<ProfilingVertex> anchorMembersToExpand) {

		ProfilingSequence partialProfilingSequence = this.profilingSequence
				.cloneWithoutGroupMembers();
		ProfilingGroupVertex clonedAnchor = this.getClonedAnchor(
				partialProfilingSequence, anchor);

		HashMap<ExecutionVertexID, ProfilingVertex> alreadyClonedVertices = new HashMap<ExecutionVertexID, ProfilingVertex>();
		for (ProfilingVertex anchorMemberToClone : anchorMembersToExpand) {
			this.cloneForward(clonedAnchor, anchorMemberToClone,
					alreadyClonedVertices);
			this.cloneBackward(clonedAnchor, anchorMemberToClone,
					alreadyClonedVertices);
		}

		return partialProfilingSequence;
	}

	private ProfilingVertex cloneBackward(ProfilingGroupVertex groupVertex,
			ProfilingVertex toClone,
			HashMap<ExecutionVertexID, ProfilingVertex> alreadyClonedVertices) {

		ProfilingVertex cloned = alreadyClonedVertices.get(toClone.getID());
		if (cloned == null) {
			cloned = new ProfilingVertex(toClone.getID(), toClone.getName());
			cloned.setQosReporter(toClone.getQosReporter());
			alreadyClonedVertices.put(toClone.getID(), cloned);
			groupVertex.addGroupMember(cloned);
		}

		for (ProfilingEdge edgeToClone : toClone.getBackwardEdges()) {
			ProfilingEdge clonedEdge = new ProfilingEdge(
					edgeToClone.getSourceChannelID(),
					edgeToClone.getTargetChannelID());
			clonedEdge.setTargetVertex(cloned);
			clonedEdge.setTargetVertexEdgeIndex(cloned.getBackwardEdges()
					.size());
			cloned.addBackwardEdge(clonedEdge);
			ProfilingVertex clonedSource = this.cloneBackward(groupVertex
					.getBackwardEdge().getSourceVertex(), edgeToClone
					.getSourceVertex(), alreadyClonedVertices);
			clonedEdge.setSourceVertex(clonedSource);
			clonedEdge.setSourceVertexEdgeIndex(clonedSource.getForwardEdges()
					.size());
			clonedSource.addForwardEdge(clonedEdge);
		}

		return cloned;
	}

	private ProfilingGroupVertex getClonedAnchor(
			ProfilingSequence clonedSequence, ProfilingGroupVertex anchor) {

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
			cloned.setQosReporter(toClone.getQosReporter());
			alreadyClonedVertices.put(toClone.getID(), cloned);
			groupVertex.addGroupMember(cloned);
		}

		for (ProfilingEdge edgeToClone : toClone.getForwardEdges()) {
			ProfilingEdge clonedEdge = new ProfilingEdge(
					edgeToClone.getSourceChannelID(),
					edgeToClone.getTargetChannelID());
			clonedEdge.setSourceVertex(cloned);
			clonedEdge
					.setSourceVertexEdgeIndex(cloned.getForwardEdges().size());
			cloned.addForwardEdge(clonedEdge);
			ProfilingVertex clonedTarget = this.cloneForward(groupVertex
					.getForwardEdge().getTargetVertex(), edgeToClone
					.getTargetVertex(), alreadyClonedVertices);
			clonedEdge.setTargetVertex(clonedTarget);
			clonedEdge.setTargetVertexEdgeIndex(clonedTarget.getBackwardEdges()
					.size());
			clonedTarget.addBackwardEdge(clonedEdge);
		}

		return cloned;
	}

	private void sendPartialProfilingSequenceToQosManager(
			InstanceConnectionInfo qosManager,
			ProfilingSequence partialSequence) throws IOException {
		int attempts = 0;
		boolean success = false;
		while (!success) {
			attempts++;
			AbstractInstance instance = this.instances.get(qosManager);
			try {
				instance.sendData(
						StreamingPluginLoader.STREAMING_PLUGIN_ID,
						new ActAsQosManagerAction(this.executionGraph
								.getJobID(), partialSequence));
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
		// FIXME
		double anchorTaskToNodeRatio = Double.MIN_VALUE;
		ProfilingGroupVertex anchorVertex = null;

		for (ProfilingGroupVertex groupVertex : this.profilingSequence
				.getSequenceVertices()) {
			double taskToNodeRatio = (double) groupVertex.getGroupMembers()
					.size() / groupVertex.getNumberOfExecutingInstances();
			if (taskToNodeRatio > anchorTaskToNodeRatio) {
				anchorTaskToNodeRatio = taskToNodeRatio;
				anchorVertex = groupVertex;
			}
		}

		return anchorVertex;
	}
}
