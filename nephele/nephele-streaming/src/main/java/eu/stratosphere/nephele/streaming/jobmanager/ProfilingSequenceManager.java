package eu.stratosphere.nephele.streaming.jobmanager;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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
import eu.stratosphere.nephele.streaming.types.TaskProfilingInfo;
import eu.stratosphere.nephele.util.StringUtils;

public class ProfilingSequenceManager implements VertexAssignmentListener {

	private static final Log LOG = LogFactory.getLog(ProfilingSequenceManager.class);

	private final ProfilingSequence profilingSequence;

	private HashMap<ExecutionVertexID, ProfilingVertex> verticesByID;

	private ExecutionGraph executionGraph;

	private int verticesWithPendingAllocation;

	private ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance> instances;

	public ProfilingSequenceManager(ProfilingSequence profilingSequence,
			ExecutionGraph executionGraph,
			ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance> instances) {

		this.profilingSequence = profilingSequence;
		this.executionGraph = executionGraph;
		this.instances = instances;
		init();
	}

	private void init() {
		this.verticesByID = new HashMap<ExecutionVertexID, ProfilingVertex>();
		for (ProfilingGroupVertex groupVertex : profilingSequence.getSequenceVertices()) {
			for (ProfilingVertex vertex : groupVertex.getGroupMembers()) {
				verticesByID.put(vertex.getID(), vertex);
				if (vertex.getProfilingDataSource() == null) {
					verticesWithPendingAllocation++;
				}
			}
		}
	}

	public void attachListenersAndPluginDataToExecutionGraph() {
		for (ExecutionVertexID vertexID : verticesByID.keySet()) {
			ExecutionVertex vertex = executionGraph.getVertexByID(vertexID);
			vertex.registerVertexAssignmentListener(this);

			TaskProfilingInfo pluginData = (TaskProfilingInfo) vertex
				.getPluginData(StreamingPluginLoader.STREAMING_PLUGIN_ID);

			if (pluginData == null) {
				pluginData = new TaskProfilingInfo(vertexID);
				vertex.setPluginData(StreamingPluginLoader.STREAMING_PLUGIN_ID, pluginData);
			}
		}
	}

	public void detachListenersAndPluginDataFromExecutionGraph() {
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
		if (vertex.getProfilingDataSource() == null) {
			verticesWithPendingAllocation--;
		}
		AbstractInstance instance = newAllocatedResource.getInstance();
		instances.putIfAbsent(instance.getInstanceConnectionInfo(), instance);
		vertex.setProfilingDataSource(instance.getInstanceConnectionInfo());
		if (verticesWithPendingAllocation == 0) {
			try {
				setupDistributedProfiling();
			} catch (Exception e) {
				LOG.error(StringUtils.stringifyException(e));
			}
		} else {
			LOG.info("verticesWithPendingAllocation=" + verticesWithPendingAllocation);
		}
	}

	private void setupDistributedProfiling() throws IOException {
		ProfilingGroupVertex anchor = determineProfilingAnchor();

		final HashMap<InstanceConnectionInfo, LinkedList<ProfilingVertex>> verticesByProfilingMaster = new HashMap<InstanceConnectionInfo, LinkedList<ProfilingVertex>>();

		for (ProfilingVertex vertex : anchor.getGroupMembers()) {
			InstanceConnectionInfo profilingMaster = vertex.getProfilingDataSource();
			LinkedList<ProfilingVertex> verticesOnProfilingMaster = verticesByProfilingMaster.get(profilingMaster);
			if (verticesOnProfilingMaster == null) {
				verticesOnProfilingMaster = new LinkedList<ProfilingVertex>();
				verticesByProfilingMaster.put(profilingMaster, verticesOnProfilingMaster);
			}
			verticesOnProfilingMaster.add(vertex);
		}

		LOG.info("Setting up distributed profiling for " + profilingSequence.toString());
		LOG.info("Number of profiling masters: " + verticesByProfilingMaster.size());
		
		for (LinkedList<ProfilingVertex> profilingMasterVertices : verticesByProfilingMaster.values()) {
			setupProfilingMaster(anchor, profilingMasterVertices);
		}

		LOG.info("Successfully set up profiling for " + profilingSequence.toString());
	}

	private void setupProfilingMaster(ProfilingGroupVertex anchor,
			LinkedList<ProfilingVertex> anchorVerticesOnProfilingMaster) throws IOException {

		InstanceConnectionInfo profilingMaster = anchorVerticesOnProfilingMaster.getFirst().getProfilingDataSource();

		LOG.info("Setting up profiling master " + profilingMaster);

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
				ExecutionVertex executionVertex = executionGraph.getVertexByID(vertex.getID());
				TaskProfilingInfo pluginData = (TaskProfilingInfo) executionVertex
					.getPluginData(StreamingPluginLoader.STREAMING_PLUGIN_ID);

				if (includeVertexInProfiling) {
					pluginData.addTaskProfilingMaster(profilingMaster);
				}

				if (!isEndVertex) {
					for (ProfilingEdge forwardEdge : vertex.getForwardEdges()) {
						pluginData.addChannelProfilingMaster(forwardEdge.getSourceChannelID(), profilingMaster);
					}
				}

				if (!isStartVertex) {
					for (ProfilingEdge backwardEdge : vertex.getBackwardEdges()) {
						pluginData.addChannelProfilingMaster(backwardEdge.getSourceChannelID(), profilingMaster);
					}
				}
			}
		}
	}

	private ProfilingSequence expandToPartialProfilingSequence(ProfilingGroupVertex anchor,
			LinkedList<ProfilingVertex> anchorMembersToExpand) {

		ProfilingSequence partialProfilingSequence = profilingSequence.cloneWithoutGroupMembers();
		ProfilingGroupVertex clonedAnchor = getClonedAnchor(partialProfilingSequence, anchor);

		for (ProfilingVertex anchorMemberToClone : anchorMembersToExpand) {
			ProfilingVertex clonedAnchorMember = cloneForward(clonedAnchor, anchorMemberToClone);
			cloneBackward(clonedAnchor, anchorMemberToClone, clonedAnchorMember);
		}

		return partialProfilingSequence;
	}

	private ProfilingVertex cloneBackward(ProfilingGroupVertex groupVertex, ProfilingVertex toClone,
			ProfilingVertex cloned) {

		if (cloned == null) {
			cloned = new ProfilingVertex(toClone.getID());
			cloned.setProfilingDataSource(toClone.getProfilingDataSource());
			groupVertex.addGroupMember(cloned);
		}

		for (ProfilingEdge edgeToClone : toClone.getBackwardEdges()) {
			ProfilingEdge clonedEdge = new ProfilingEdge(edgeToClone.getSourceChannelID(),
				edgeToClone.getTargetChannelID());
			clonedEdge.setTargetVertex(cloned);
			clonedEdge.setTargetVertexEdgeIndex(cloned.getBackwardEdges().size());
			cloned.addBackwardEdge(clonedEdge);
			ProfilingVertex clonedSource = cloneBackward(groupVertex.getBackwardEdge().getSourceVertex(),
				edgeToClone.getSourceVertex(), null);
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
			ProfilingVertex toClone) {

		ProfilingVertex cloned = new ProfilingVertex(toClone.getID());
		cloned.setProfilingDataSource(toClone.getProfilingDataSource());
		groupVertex.addGroupMember(cloned);

		for (ProfilingEdge edgeToClone : toClone.getForwardEdges()) {
			ProfilingEdge clonedEdge = new ProfilingEdge(edgeToClone.getSourceChannelID(),
				edgeToClone.getTargetChannelID());
			clonedEdge.setSourceVertex(cloned);
			clonedEdge.setSourceVertexEdgeIndex(cloned.getForwardEdges().size());
			cloned.addForwardEdge(clonedEdge);
			ProfilingVertex clonedTarget = cloneForward(groupVertex.getForwardEdge().getTargetVertex(),
				edgeToClone.getTargetVertex());
			clonedEdge.setTargetVertex(clonedTarget);
			clonedEdge.setTargetVertexEdgeIndex(clonedTarget.getBackwardEdges().size());
			clonedTarget.addBackwardEdge(clonedEdge);
		}

		return cloned;
	}

	private void sendPartialProfilingSequenceToProfilingMaster(InstanceConnectionInfo profilingMaster,
			ProfilingSequence partialSequence) throws IOException {

		AbstractInstance instance = instances.get(profilingMaster);
		instance.sendData(StreamingPluginLoader.STREAMING_PLUGIN_ID,
				new ActAsProfilingMasterAction(executionGraph.getJobID(), partialSequence));
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
