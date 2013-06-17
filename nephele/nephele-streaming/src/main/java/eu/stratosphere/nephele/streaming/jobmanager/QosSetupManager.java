package eu.stratosphere.nephele.streaming.jobmanager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGate;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.VertexAssignmentListener;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.AbstractJobInputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.WrapperUtils;
import eu.stratosphere.nephele.streaming.util.StreamUtil;
import eu.stratosphere.nephele.util.StringUtils;

public class QosSetupManager implements VertexAssignmentListener {

	private static final Log LOG = LogFactory.getLog(QosSetupManager.class);

	private ExecutionGraph executionGraph;

	private ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance> instances;

	private JobID jobID;

	private List<JobGraphLatencyConstraint> constraints;

	private HashSet<JobVertexID> constrainedJobVertices;

	private HashSet<ExecutionVertexID> verticesWithPendingAllocation;

	public QosSetupManager(JobID jobID,
			List<JobGraphLatencyConstraint> constraints) {
		this.jobID = jobID;
		this.constraints = constraints;
		this.constrainedJobVertices = computeJobVerticesToRewrite();
	}

	public void registerOnExecutionGraph(ExecutionGraph executionGraph) {
		this.executionGraph = executionGraph;
		this.instances = new ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance>();
		this.attachAssignmentListenersToExecutionGraph();
	}

	private void attachAssignmentListenersToExecutionGraph() {
		this.verticesWithPendingAllocation = new HashSet<ExecutionVertexID>();
		Set<ExecutionVertexID> visited = new HashSet<ExecutionVertexID>();

		for (int i = 0; i < this.executionGraph.getNumberOfInputVertices(); i++) {
			ExecutionVertex inputVertex = this.executionGraph.getInputVertex(i);
			visited.add(inputVertex.getID());
			attachAssignmentListenersToReachableVertices(inputVertex, visited);
		}
	}

	private void attachAssignmentListenersToReachableVertices(
			ExecutionVertex vertex, Set<ExecutionVertexID> visited) {

		if (visited.contains(vertex.getID())) {
			return;
		}
		visited.add(vertex.getID());

		if (this.constrainedJobVertices.contains(vertex.getGroupVertex()
				.getJobVertexID())) {
			vertex.registerVertexAssignmentListener(this);
			this.verticesWithPendingAllocation.add(vertex.getID());
		}

		for (int i = 0; i < vertex.getNumberOfOutputGates(); i++) {
			ExecutionGate outputGate = vertex.getOutputGate(i);
			for (int j = 0; j < outputGate.getNumberOfEdges(); j++) {
				ExecutionVertex nextVertex = outputGate.getEdge(j)
						.getInputGate().getVertex();
				attachAssignmentListenersToReachableVertices(nextVertex,
						visited);
			}
		}
	}

	public void shutdown() {
		this.instances.clear();
		this.executionGraph = null;
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public List<JobGraphLatencyConstraint> getConstraints() {
		return this.constraints;
	}

	/**
	 * Replaces the input/output/task classes of vertices inside the given job
	 * graph with wrapped versions. This is done only for the vertices that are
	 * affected by a constraint. The input/output/task classes contain the user
	 * defined code. This method wraps the user defined code and adds streaming
	 * framework calls that record latencies, can perform task chaining etc.
	 * 
	 * @param jobGraph
	 */
	public void rewriteJobGraph(JobGraph jobGraph) {
		rewriteInputVertices(jobGraph);
		rewriteTaskVertices(jobGraph);
		rewriteOutputVertices(jobGraph);
	}

	private HashSet<JobVertexID> computeJobVerticesToRewrite() {
		HashSet<JobVertexID> verticesToRewrite = new HashSet<JobVertexID>();

		for (JobGraphLatencyConstraint constraint : this.constraints) {
			verticesToRewrite.addAll(constraint.getSequence()
					.getVerticesInSequence());

			if (constraint.getSequence().getFirst().isEdge()) {
				verticesToRewrite.add(constraint.getSequence().getFirst()
						.getSourceVertexID());
			}

			if (constraint.getSequence().getLast().isEdge()) {
				verticesToRewrite.add(constraint.getSequence().getLast()
						.getTargetVertexID());
			}
		}

		return verticesToRewrite;
	}

	private void rewriteOutputVertices(JobGraph jobGraph) {

		for (AbstractJobOutputVertex vertex : StreamUtil.toIterable(jobGraph
				.getOutputVertices())) {

			if (!this.constrainedJobVertices.contains(vertex.getID())) {
				continue;
			}

			if (!(vertex instanceof JobOutputVertex)) {
				LOG.warn("Cannot wrap output vertex of type "
						+ vertex.getClass().getName() + ", skipping...");
				continue;
			}
			WrapperUtils.wrapOutputClass((JobOutputVertex) vertex);
		}
	}

	private void rewriteInputVertices(JobGraph jobGraph) {

		for (AbstractJobInputVertex inputVertex : StreamUtil
				.toIterable(jobGraph.getInputVertices())) {

			if (!this.constrainedJobVertices.contains(inputVertex.getID())) {
				continue;
			}

			if (!(inputVertex instanceof JobInputVertex)) {
				LOG.warn("Cannot wrap input vertex of type "
						+ inputVertex.getClass().getName() + ", skipping...");
				continue;
			}
			WrapperUtils.wrapInputClass((JobInputVertex) inputVertex);
		}
	}

	private void rewriteTaskVertices(JobGraph jobGraph) {

		for (JobTaskVertex vertex : StreamUtil.toIterable(jobGraph
				.getTaskVertices())) {
			if (!this.constrainedJobVertices.contains(vertex.getID())) {
				continue;
			}

			WrapperUtils.wrapTaskClass(vertex);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.nephele.executiongraph.VertexAssignmentListener#
	 * vertexAssignmentChanged
	 * (eu.stratosphere.nephele.executiongraph.ExecutionVertexID,
	 * eu.stratosphere.nephele.instance.AllocatedResource)
	 */
	@Override
	public synchronized void vertexAssignmentChanged(ExecutionVertexID id,
			AllocatedResource newAllocatedResource) {

		this.verticesWithPendingAllocation.remove(id);
		AbstractInstance instance = newAllocatedResource.getInstance();
		this.instances.putIfAbsent(instance.getInstanceConnectionInfo(),
				instance);
		if (this.verticesWithPendingAllocation.isEmpty()) {
			computeAndDistributeQosSetup();
		}
	}

	private void computeAndDistributeQosSetup() {
		QosSetup qosSetup = new QosSetup(this.executionGraph, this.constraints);
		qosSetup.computeQosRoles();
		qosSetup.attachRolesToExecutionGraph();
		
		// FIXME: continue here: distribute QoS setup
	}
	
//	private void setupQosManagers() throws Exception {
//		final QosGroupVertex anchor = this.determineProfilingAnchor();
//
//		final HashMap<InstanceConnectionInfo, LinkedList<QosVertex>> verticesByQosManager = new HashMap<InstanceConnectionInfo, LinkedList<QosVertex>>();
//
//		for (QosVertex vertex : anchor.getMembers()) {
//			InstanceConnectionInfo qosManager = vertex
//					.getExecutingInstance();
//			LinkedList<QosVertex> verticesOnQosManager = verticesByQosManager
//					.get(qosManager);
//			if (verticesOnQosManager == null) {
//				verticesOnQosManager = new LinkedList<QosVertex>();
//				verticesByQosManager.put(qosManager,
//						verticesOnQosManager);
//			}
//			verticesOnQosManager.add(vertex);
//		}
//
//		ExecutorService threadPool = Executors.newFixedThreadPool(8);
//		final AtomicReference<Exception> exceptionCollector = new AtomicReference<Exception>();
//
//		for (final LinkedList<QosVertex> qosManagerVertices : verticesByQosManager
//				.values()) {
//			threadPool.execute(new Runnable() {
//				@Override
//				public void run() {
//					try {
//						ProfilingSequenceManager.this.setupQosManager(
//								anchor, qosManagerVertices);
//					} catch (Exception e) {
//						LOG.error(StringUtils.stringifyException(e));
//						exceptionCollector.set(e);
//					}
//				}
//			});
//		}
//		threadPool.shutdown();
//
//		try {
//			threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
//			if (exceptionCollector.get() != null) {
//				throw exceptionCollector.get();
//			}
//			LOG.info("Successfully set up profiling masters for "
//					+ this.profilingSequence.toString());
//		} catch (InterruptedException e) {
//			LOG.info("Interrupted while setting up profiling masters for "
//					+ this.profilingSequence.toString());
//			threadPool.shutdownNow();
//		}
//	}

}
