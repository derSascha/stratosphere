package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionEdge;
import eu.stratosphere.nephele.executiongraph.ExecutionGate;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupEdge;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;

public class ProfilingGraphFactory {

	/**
	 * Creates a list of {@link ProfilingSequence} objects, by traversing the
	 * execution graph and collecting all paths that begin at startVertex and
	 * end at endVertex. Each path is then converted to a
	 * {@link ProfilingSequence} that may or may not include the start and end
	 * vertex, depending on includeStartVertex and includeEndVertex. There are
	 * no vertex or edge objects shared between the resulting
	 * {@link ProfilingSequence} objects, i.e. you can safely change the objects
	 * in one sequence without affecting other sequences.
	 * 
	 * @param executionGraph
	 *            The the execution graph to traverse.
	 * @param startVertex
	 *            The vertex to start the execution graph traversal from.
	 * @param includeStartVertex
	 *            Whether the {@link ProfilingGroupVertex} objects corresponding
	 *            to startVertex should be included in the resulting sequences.
	 * @param endVertex
	 *            The vertex to end the execution graph traversal at.
	 * @param includeEndVertex
	 *            Whether the {@link ProfilingGroupVertex} objects corresponding
	 *            to endVertex should be included in the resulting sequences.
	 * @return
	 */
	public static List<ProfilingSequence> createProfilingSequences(
			ExecutionGroupVertex startVertex, boolean includeStartVertex,
			ExecutionGroupVertex endVertex, boolean includeEndVertex) {

		LinkedList<ExecutionGroupVertex> initialPath = new LinkedList<ExecutionGroupVertex>();
		initialPath.add(startVertex);
		List<List<ExecutionGroupVertex>> completePaths = new LinkedList<List<ExecutionGroupVertex>>();
		depthFirstSearchExecutionGraph(initialPath, completePaths, endVertex);

		return toProfilingGroupElementSequences(completePaths,
				includeStartVertex, includeEndVertex);
	}

	private static List<ProfilingSequence> toProfilingGroupElementSequences(
			List<List<ExecutionGroupVertex>> executionGroupPaths,
			boolean includeStartVertex, boolean includeEndVertex) {

		LinkedList<ProfilingSequence> profilingGroupElementSequences = new LinkedList<ProfilingSequence>();
		for (List<ExecutionGroupVertex> executionGroupPath : executionGroupPaths) {
			profilingGroupElementSequences.add(toProfilingGroupElementSequence(
					executionGroupPath, includeStartVertex, includeEndVertex));
		}

		return profilingGroupElementSequences;
	}

	private static ProfilingSequence toProfilingGroupElementSequence(
			List<ExecutionGroupVertex> executionGroupPath,
			boolean includeStartVertex, boolean includeEndVertex) {

		ProfilingSequence sequence = new ProfilingSequence();
		sequence.setIncludeStartVertex(includeStartVertex);
		sequence.setIncludeEndVertex(includeEndVertex);

		ExecutionGroupVertex lastExecutionGroupVertex = null;
		ProfilingGroupVertex lastProfilingGroupVertex = null;

		for (ExecutionGroupVertex executionGroupVertex : executionGroupPath) {
			ProfilingGroupVertex profilingGroupVertex = new ProfilingGroupVertex(
					executionGroupVertex.getJobVertexID(),
					executionGroupVertex.getName());
			sequence.addSequenceVertex(profilingGroupVertex);

			createGroupMembers(profilingGroupVertex, executionGroupVertex);

			if (lastProfilingGroupVertex != null) {
				// FIXME this does not work with multigraphs (two group vertices
				// connected by more than one group edge)
				ExecutionGroupEdge executionGroupEdge = executionGroupVertex
						.getBackwardEdges(lastExecutionGroupVertex).get(0);

				ProfilingGroupEdge profilingGroupEdge = new ProfilingGroupEdge(
						executionGroupEdge.getDistributionPattern(),
						lastProfilingGroupVertex, profilingGroupVertex);
				lastProfilingGroupVertex.setForwardEdge(profilingGroupEdge);
				profilingGroupVertex.setBackwardEdge(profilingGroupEdge);

				connectGroupMembers(profilingGroupEdge, executionGroupEdge);
			}

			lastExecutionGroupVertex = executionGroupVertex;
			lastProfilingGroupVertex = profilingGroupVertex;
		}

		return sequence;
	}

	/**
	 * Populates the source and target vertices of profilingGroupEdge with
	 * {@link ProfilingEdge} objects by duplicating the {@link ExecutionEdge}
	 * objects executionGroupEdge stands for.
	 * 
	 * @param profilingGroupEdge
	 * @param executionGroupEdge
	 */
	private static void connectGroupMembers(
			ProfilingGroupEdge profilingGroupEdge,
			ExecutionGroupEdge executionGroupEdge) {

		int sourceMembers = executionGroupEdge.getSourceVertex()
				.getCurrentNumberOfGroupMembers();
		for (int i = 0; i < sourceMembers; i++) {
			ExecutionGate executionOutputGate = executionGroupEdge
					.getSourceVertex().getGroupMember(i)
					.getOutputGate(executionGroupEdge.getIndexOfOutputGate());

			for (int j = 0; j < executionOutputGate.getNumberOfEdges(); j++) {
				ExecutionEdge executionEdge = executionOutputGate.getEdge(j);

				ProfilingEdge profilingEdge = new ProfilingEdge(
						executionEdge.getOutputChannelID(),
						executionEdge.getInputChannelID());
				ProfilingVertex sourceVertex = profilingGroupEdge
						.getSourceVertex().getGroupMembers().get(i);
				ProfilingVertex targetVertex = profilingGroupEdge
						.getTargetVertex()
						.getGroupMembers()
						.get(executionEdge.getInputGate().getVertex()
								.getIndexInVertexGroup());
				profilingEdge.setSourceVertex(sourceVertex);
				profilingEdge.setTargetVertex(targetVertex);
				sourceVertex.addForwardEdge(profilingEdge);
				profilingEdge.setSourceVertexEdgeIndex(sourceVertex
						.getForwardEdges().size() - 1);
				targetVertex.addBackwardEdge(profilingEdge);
				profilingEdge.setTargetVertexEdgeIndex(targetVertex
						.getBackwardEdges().size() - 1);
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
	private static void createGroupMembers(
			ProfilingGroupVertex profilingGroupVertex,
			ExecutionGroupVertex executionGroupVertex) {

		for (int i = 0; i < executionGroupVertex
				.getCurrentNumberOfGroupMembers(); i++) {
			ExecutionVertex executionVertex = executionGroupVertex
					.getGroupMember(i);
			profilingGroupVertex.addGroupMember(new ProfilingVertex(
					executionVertex.getID(), executionVertex.getName()
							+ executionVertex.getIndexInVertexGroup()));
		}
	}

	/**
	 * Performs a recursive depth first search for pathEnd starting at the last
	 * vertex of currentPath, accumulating all paths found to end in pathEnd in
	 * completePaths.
	 * 
	 * @param currentPath
	 *            Initial path with at least one element to start with (will be
	 *            altered during recursive search).
	 * @param completePaths
	 *            Accumulates the paths found to end at pathEnd
	 * @param pathEnd
	 *            A path is considered a complete path, when it ends in pathEnd
	 *            (termination condition for depth first search).
	 */
	private static void depthFirstSearchExecutionGraph(
			LinkedList<ExecutionGroupVertex> currentPath,
			List<List<ExecutionGroupVertex>> completePaths,
			ExecutionGroupVertex pathEnd) {

		ExecutionGroupVertex currentPathEnd = currentPath.getLast();

		for (int i = 0; i < currentPathEnd.getNumberOfForwardLinks(); i++) {
			ExecutionGroupVertex successor = currentPathEnd.getForwardEdge(i)
					.getTargetVertex();
			currentPath.add(successor);

			if (successor == pathEnd) {
				completePaths.add(new LinkedList<ExecutionGroupVertex>(
						currentPath));
			} else {
				depthFirstSearchExecutionGraph(currentPath, completePaths,
						pathEnd);
			}
			currentPath.removeLast();
		}
	}

}
