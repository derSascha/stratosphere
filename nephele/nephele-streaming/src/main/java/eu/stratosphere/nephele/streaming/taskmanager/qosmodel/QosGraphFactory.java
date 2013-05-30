package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import eu.stratosphere.nephele.executiongraph.ExecutionEdge;
import eu.stratosphere.nephele.executiongraph.ExecutionGate;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupEdge;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.SequenceElement;

public class QosGraphFactory {

	/**
	 * Builds the smallest possible subgraph of the given execution graph, where
	 * all vertices and edges are affected by the given constraint. If the
	 * constraint starts/ends with and edge the respective source/target vertex
	 * is also part of the QosGraph, although it is not strictly part of the
	 * constraint.
	 * 
	 * @param execGraph
	 *            An execution graph.
	 * @param constraint
	 *            A latency constraint the affects elements of the given
	 *            execution graph.
	 * 
	 * @return A {@link QosGraph} that contains all those vertices and edges of
	 *         the given execution graph, that are covered by the given latency
	 *         constraint.
	 */
	public static QosGraph createConstrainedQosGraph(ExecutionGraph execGraph,
			JobGraphLatencyConstraint constraint) {

		QosGroupVertex startVertex = null;
		ExecutionGroupVertex currExecVertex = null;
		QosGroupVertex currGroupVertex = null;

		for (SequenceElement<JobVertexID> sequenceElem : constraint
				.getSequence()) {

			if (currExecVertex == null) {
				JobVertexID firstJobVertexID;

				if (sequenceElem.isVertex()) {
					firstJobVertexID = sequenceElem.getVertexID();
				} else {
					firstJobVertexID = sequenceElem.getSourceVertexID();
				}
				currExecVertex = findGroupVertex(execGraph, firstJobVertexID);
				currGroupVertex = toQosGroupVertex(currExecVertex);
				startVertex = currGroupVertex;
			}

			if (sequenceElem.isEdge()) {
				ExecutionGroupEdge execEdge = currExecVertex
						.getForwardEdge(sequenceElem.getOutputGateIndex());

				ExecutionGroupVertex nextExecVertex = execEdge
						.getTargetVertex();
				QosGroupVertex nextGroupVertex = toQosGroupVertex(nextExecVertex);

				wireTo(currGroupVertex, nextGroupVertex, execEdge);
				currGroupVertex = nextGroupVertex;
			}
		}

		return new QosGraph(startVertex, constraint);
	}

	private static void wireTo(QosGroupVertex from, QosGroupVertex to,
			ExecutionGroupEdge execEdge) {

		int outputGate = execEdge.getIndexOfOutputGate();
		int inputGate = execEdge.getIndexOfInputGate();

		QosGroupEdge qosEdge = new QosGroupEdge(
				execEdge.getDistributionPattern(), from, to, outputGate,
				inputGate);

		connectGroupMembers(qosEdge, execEdge);
	}

	private static QosGroupVertex toQosGroupVertex(
			ExecutionGroupVertex execVertex) {

		QosGroupVertex qosGroupVertex = new QosGroupVertex(
				execVertex.getJobVertexID(), execVertex.getName());
		createGroupMembers(qosGroupVertex, execVertex);

		return qosGroupVertex;
	}

	private static ExecutionGroupVertex findGroupVertex(
			ExecutionGraph execGraph, JobVertexID jobVertexID) {

		ExecutionStage stage = execGraph.getStage(0);
		for (int i = 0; i < stage.getNumberOfStageMembers(); i++) {

			ExecutionGroupVertex stageMember = stage.getStageMember(i);
			if (stageMember.getJobVertexID().equals(jobVertexID)) {
				return stageMember;
			}
		}

		throw new RuntimeException(
				"Could not find execution group vertex for given job vertex id. This is a bug.");
	}

	/**
	 * Duplicates the execution edgs and gates of the given execution group edge
	 * and links them to the given QoS group edge.
	 * 
	 */
	private static void connectGroupMembers(QosGroupEdge qosGroupEdge,
			ExecutionGroupEdge execGroupEdge) {

		int sourceMembers = execGroupEdge.getSourceVertex()
				.getCurrentNumberOfGroupMembers();

		for (int i = 0; i < sourceMembers; i++) {

			ExecutionGate execOutputGate = execGroupEdge.getSourceVertex()
					.getGroupMember(i)
					.getOutputGate(execGroupEdge.getIndexOfOutputGate());

			QosVertex sourceVertex = qosGroupEdge.getSourceVertex()
					.getMembers().get(i);
			QosGate qosOutputGate = new QosGate(sourceVertex,
					qosGroupEdge.getOutputGateIndex());
			sourceVertex.setOutputGate(qosOutputGate);

			for (int j = 0; j < execOutputGate.getNumberOfEdges(); j++) {
				ExecutionEdge executionEdge = execOutputGate.getEdge(j);

				QosEdge qosEdge = new QosEdge(
						executionEdge.getOutputChannelID(),
						executionEdge.getInputChannelID());

				QosVertex targetVertex = qosGroupEdge
						.getTargetVertex()
						.getMembers()
						.get(executionEdge.getInputGate().getVertex()
								.getIndexInVertexGroup());

				QosGate qosInputGate = new QosGate(targetVertex,
						qosGroupEdge.getInputGateIndex());
				targetVertex.setInputGate(qosInputGate);

				qosEdge.setOutputGate(qosOutputGate);
				qosEdge.setInputGate(qosInputGate);
			}
		}
	}

	/**
	 * Populates profilingGroupVertex with {@link ProfilingVertex} objects, by
	 * duplicating the members found in executionGroupVertex.
	 * 
	 * @param profilingGroupVertex
	 * @param executionGroupVertex
	 */
	private static void createGroupMembers(QosGroupVertex profilingGroupVertex,
			ExecutionGroupVertex executionGroupVertex) {

		for (int i = 0; i < executionGroupVertex
				.getCurrentNumberOfGroupMembers(); i++) {
			ExecutionVertex executionVertex = executionGroupVertex
					.getGroupMember(i);

			profilingGroupVertex.addGroupMember(new QosVertex(executionVertex
					.getID(), executionVertex.getName()
					+ executionVertex.getIndexInVertexGroup(), executionVertex
					.getAllocatedResource().getInstance()
					.getInstanceConnectionInfo()));
		}
	}
}
