/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.nephele.streaming.jobmanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGate;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphFactory;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

/**
 * This class implements the algorithms from Section 3.4.2 from the following
 * paper Lohrmann,Warneke,Kao: "Nephele Streaming: Stream Processing under QoS
 * Constraints at Scale" (to appear in Journal of Cluster Computing, Springer
 * US).
 * 
 * 
 * @author Bjoern Lohrmann
 */
public class QosSetup {

	private ExecutionGraph executionGraph;

	private List<JobGraphLatencyConstraint> constraints;

	private HashMap<LatencyConstraintID, QosGraph> qosGraphs;

	private HashMap<InstanceConnectionInfo, InstanceQosRoles> qosRoles;

	public QosSetup(ExecutionGraph executionGraph,
			List<JobGraphLatencyConstraint> constraints) {
		this.executionGraph = executionGraph;
		this.constraints = constraints;
	}

	public void computeQosRoles() {
		createQosGraphs();
		computeQosManagerRoles();
		computeQosReporterRoles();
	}

	private void computeQosReporterRoles() {
		for (InstanceQosRoles instanceRoles : this.qosRoles.values()) {
			for (QosManagerRole qosManager : instanceRoles.getManagerRoles()) {
				computeReportersForManager(qosManager);
			}
		}
	}

	private void computeReportersForManager(QosManagerRole qosManager) {
		JobGraphSequence sequence = qosManager.getQosGraph()
				.getConstraintByID(qosManager.getConstraintID()).getSequence();

		// depending on the constraint's sequence, the anchor vertex may be part
		// of the constraint or not.
		boolean anchorVertexIsConstrained = sequence.isInSequence(qosManager
				.getAnchorVertex().getJobVertexID());

		Deque<SequenceElement<JobVertexID>> afterDeque = getSequenceAfterIncludingAnchor(
				sequence, qosManager.getAnchorVertex().getJobVertexID());

		Deque<SequenceElement<JobVertexID>> beforeDeque = getSequenceBeforeAnchor(
				sequence, qosManager.getAnchorVertex().getJobVertexID());

		for (QosVertex anchorMember : qosManager.getMembersOnInstance()) {
			if (anchorVertexIsConstrained) {
				forwardComputeReporters(qosManager, anchorMember, afterDeque);
			} else if (!afterDeque.isEmpty()) {
				QosGate outputGate = anchorMember.getOutputGate(afterDeque
						.getFirst().getOutputGateIndex());
				for (QosEdge edge : outputGate.getEdges()) {
					forwardComputeReporters(qosManager, edge, afterDeque);
				}
			}

			if (!beforeDeque.isEmpty()) {
				QosGate inputGate = anchorMember.getInputGate(beforeDeque
						.getFirst().getInputGateIndex());
				for (QosEdge edge : inputGate.getEdges()) {
					backwardComputeReporters(qosManager, edge, beforeDeque);
				}
			}
		}
	}
	
	private void forwardComputeReporters(QosManagerRole qosManager,
			QosVertex vertex,
			Deque<SequenceElement<JobVertexID>> sequenceDeque) {

		SequenceElement<JobVertexID> currentElem = sequenceDeque.removeFirst();

		addReporterForQosVertex(qosManager, vertex, currentElem);

		if (!sequenceDeque.isEmpty()) {
			QosGate outputGate = vertex.getOutputGate(sequenceDeque
					.getFirst().getOutputGateIndex());
			for (QosEdge edge : outputGate.getEdges()) {
				forwardComputeReporters(qosManager, edge, sequenceDeque);
			}
		}

		sequenceDeque.addFirst(currentElem);
	}

	private void forwardComputeReporters(QosManagerRole qosManager,
			QosEdge edge, Deque<SequenceElement<JobVertexID>> sequenceDeque) {

		SequenceElement<JobVertexID> currentElem = sequenceDeque.removeFirst();

		addReportersForQosEdge(qosManager, edge);

		if (!sequenceDeque.isEmpty()) {
			forwardComputeReporters(qosManager,
					edge.getInputGate().getVertex(), sequenceDeque);
		}

		sequenceDeque.addFirst(currentElem);
	}


	private void backwardComputeReporters(QosManagerRole qosManager,
			QosVertex currentMember,
			Deque<SequenceElement<JobVertexID>> sequenceDeque) {

		SequenceElement<JobVertexID> currentElem = sequenceDeque.removeFirst();

		addReporterForQosVertex(qosManager, currentMember, currentElem);

		if (!sequenceDeque.isEmpty()) {
			QosGate inputGate = currentMember.getInputGate(sequenceDeque
					.getFirst().getInputGateIndex());
			for (QosEdge edge : inputGate.getEdges()) {
				backwardComputeReporters(qosManager, edge, sequenceDeque);
			}
		}

		sequenceDeque.addFirst(currentElem);
	}

	private void backwardComputeReporters(QosManagerRole qosManager,
			QosEdge edge, Deque<SequenceElement<JobVertexID>> sequenceDeque) {

		SequenceElement<JobVertexID> currentElem = sequenceDeque.removeFirst();

		addReportersForQosEdge(qosManager, edge);

		if (!sequenceDeque.isEmpty()) {
			backwardComputeReporters(qosManager,
					edge.getOutputGate().getVertex(), sequenceDeque);
		}

		sequenceDeque.addFirst(currentElem);
	}
	
	private void addReporterForQosVertex(QosManagerRole qosManager,
			QosVertex vertex, SequenceElement<JobVertexID> sequenceElem) {
		
		InstanceConnectionInfo reporterInstance = vertex.getExecutingInstance();

		QosReporterRole reporterRole = new QosReporterRole(vertex.getID(),
				sequenceElem.getInputGateIndex(),
				sequenceElem.getOutputGateIndex(),
				qosManager.getManagerInstance());

		getOrCreateInstanceRoles(reporterInstance)
				.addReporterRole(reporterRole);
	}


	private void addReportersForQosEdge(QosManagerRole qosManager, QosEdge edge) {
		InstanceConnectionInfo srcReporterInstance = edge.getOutputGate()
				.getVertex().getExecutingInstance();
		InstanceConnectionInfo targetReporterInstance = edge.getInputGate()
				.getVertex().getExecutingInstance();

		QosReporterRole reporterRole = new QosReporterRole(
				edge.getSourceChannelID(), qosManager.getManagerInstance());
		getOrCreateInstanceRoles(srcReporterInstance).addReporterRole(
				reporterRole);
		getOrCreateInstanceRoles(targetReporterInstance).addReporterRole(
				reporterRole);
	}

	private LinkedList<SequenceElement<JobVertexID>> getSequenceBeforeAnchor(
			JobGraphSequence sequence, JobVertexID anchorVertexID) {

		LinkedList<SequenceElement<JobVertexID>> ret = new LinkedList<SequenceElement<JobVertexID>>();

		SequenceElement<JobVertexID> first = sequence.getFirst();
		boolean noElementsBeforeAnchor = (first.isVertex() && first
				.getVertexID().equals(anchorVertexID))
				|| (first.isEdge() && first.getSourceVertexID().equals(
						anchorVertexID));

		if (noElementsBeforeAnchor) {
			return ret;
		}

		ret = new LinkedList<SequenceElement<JobVertexID>>(sequence);
		Collections.reverse(ret);
		while (!ret.isEmpty()) {
			SequenceElement<JobVertexID> current = ret.getFirst();

			if (current.isEdge()
					&& current.getTargetVertexID().equals(anchorVertexID)) {
				break;
			}

			ret.removeFirst();
		}

		return ret;
	}

	private LinkedList<SequenceElement<JobVertexID>> getSequenceAfterIncludingAnchor(
			JobGraphSequence sequence, JobVertexID anchorVertexID) {

		LinkedList<SequenceElement<JobVertexID>> ret = new LinkedList<SequenceElement<JobVertexID>>();

		SequenceElement<JobVertexID> last = sequence.getLast();
		boolean noElementsAfterIncludingAnchor = last.isEdge()
				&& last.getTargetVertexID().equals(anchorVertexID);

		if (noElementsAfterIncludingAnchor) {
			return ret;
		}

		ret = new LinkedList<SequenceElement<JobVertexID>>(sequence);

		while (!ret.isEmpty()) {
			SequenceElement<JobVertexID> current = ret.getFirst();

			if ((current.isVertex() && current.getVertexID().equals(
					anchorVertexID))
					|| (current.isEdge() && current.getSourceVertexID().equals(
							anchorVertexID))) {
				break;
			}

			ret.removeFirst();
		}

		return ret;
	}


	private void createQosGraphs() {
		this.qosGraphs = new HashMap<LatencyConstraintID, QosGraph>();

		for (JobGraphLatencyConstraint constraint : this.constraints) {
			this.qosGraphs
					.put(constraint.getID(), QosGraphFactory
							.createConstrainedQosGraph(this.executionGraph,
									constraint));
		}
	}

	/**
	 * Computes which instances shall run QosManagers.
	 */
	private void computeQosManagerRoles() {
		this.qosRoles = new HashMap<InstanceConnectionInfo, InstanceQosRoles>();

		for (QosGraph qosGraph : this.qosGraphs.values()) {
			QosGroupVertex anchorVertex = getAnchorVertex(qosGraph);

			for (List<QosVertex> membersOnInstance : partitionMembersByInstance(anchorVertex)) {
				InstanceConnectionInfo instance = membersOnInstance.get(0)
						.getExecutingInstance();

				QosManagerRole managerRole = new QosManagerRole(qosGraph,
						qosGraph.getConstraints().iterator().next().getID(),
						anchorVertex, membersOnInstance);
				getOrCreateInstanceRoles(instance).addManagerRole(managerRole);
			}
		}

	}

	private InstanceQosRoles getOrCreateInstanceRoles(
			InstanceConnectionInfo instance) {

		InstanceQosRoles instanceRoles = this.qosRoles.get(instance);
		if (instanceRoles == null) {
			instanceRoles = new InstanceQosRoles(instance);
			this.qosRoles.put(instance, instanceRoles);
		}

		return instanceRoles;
	}

	private Iterable<List<QosVertex>> partitionMembersByInstance(
			QosGroupVertex anchorVertex) {
		HashMap<InstanceConnectionInfo, List<QosVertex>> members = new HashMap<InstanceConnectionInfo, List<QosVertex>>();
		for (QosVertex member : anchorVertex.getMembers()) {
			InstanceConnectionInfo instance = member.getExecutingInstance();

			List<QosVertex> membersOnInstance = members.get(instance);
			if (membersOnInstance == null) {
				membersOnInstance = new ArrayList<QosVertex>();
				members.put(instance, membersOnInstance);
			}

			membersOnInstance.add(member);
		}

		return members.values();
	}

	/**
	 * Finds the anchor vertex for the constraint of the given Qos graph. The
	 * anchor vertex is the group vertex on the constraint's sequence, that has
	 * the maximum worker count. If this is not a unique choice, the anchor
	 * candidate is chosen with that has the (constrained) group edge with the
	 * lowest number of channels.
	 * 
	 * @param qosGraph
	 *            Provides the graph structure and the constraint.
	 * @return The chosen anchor vertex.
	 */
	private QosGroupVertex getAnchorVertex(QosGraph qosGraph) {
		Set<JobVertexID> anchorCandidates = collectAnchorCandidates(qosGraph);

		retainCandidatesWithMaxInstanceCount(anchorCandidates, qosGraph);
		retainCandidatesWithMinChannelCountOnSequence(anchorCandidates,
				qosGraph);

		return qosGraph.getGroupVertexByID(anchorCandidates.iterator().next());
	}

	private void retainCandidatesWithMinChannelCountOnSequence(
			Set<JobVertexID> anchorCandidates, QosGraph qosGraph) {

		HashMap<JobVertexID, Integer> channelCounts = new HashMap<JobVertexID, Integer>();
		int minChannelCount = countChannelsOnSequence(qosGraph,
				anchorCandidates, channelCounts);

		Iterator<JobVertexID> candidateIter = anchorCandidates.iterator();
		while (candidateIter.hasNext()) {
			JobVertexID curr = candidateIter.next();
			if (channelCounts.get(curr) > minChannelCount) {
				candidateIter.remove();
			}
		}
	}

	/**
	 * For each anchor candidate (see anchor candidates), it finds the channel
	 * count of the ingoing/outgoing edge on the constraint's sequence, that has
	 * the lowest channel count.
	 * 
	 * @param qosGraph
	 *            Provides the graph structure and constraint.
	 * @param anchorCandidates
	 *            Defines the group vertices that are anchor candidates.
	 * @param channelCounts
	 *            Accumulates the channel counts for the group vertices that are
	 *            anchor candidates. This is part of the result.
	 * @return the lowest channel count found among the anchor candidates.
	 */
	private int countChannelsOnSequence(QosGraph qosGraph,
			Set<JobVertexID> anchorCandidates,
			HashMap<JobVertexID, Integer> channelCounts) {

		int minChannelCount = Integer.MAX_VALUE;
		for (SequenceElement<JobVertexID> sequenceElem : qosGraph
				.getConstraints().iterator().next().getSequence()) {

			if (sequenceElem.isEdge()) {
				JobVertexID sourceID = sequenceElem.getSourceVertexID();
				JobVertexID targetID = sequenceElem.getTargetVertexID();
				QosGroupVertex source = qosGraph.getGroupVertexByID(sourceID);
				QosGroupVertex target = qosGraph.getGroupVertexByID(targetID);
				DistributionPattern distPattern = source.getForwardEdge(
						sequenceElem.getOutputGateIndex())
						.getDistributionPattern();

				int channelCount = countChannelsBetweenGroupVertices(source,
						target, distPattern);

				if (anchorCandidates.contains(sourceID)) {
					int sourceChannelCount = updateMinChannelCount(
							channelCounts, sourceID, channelCount);
					minChannelCount = Math.min(minChannelCount,
							sourceChannelCount);
				}

				if (anchorCandidates.contains(targetID)) {
					int targetChannelCount = updateMinChannelCount(
							channelCounts, targetID, channelCount);
					minChannelCount = Math.min(minChannelCount,
							targetChannelCount);
				}
			}

		}
		return minChannelCount;
	}

	private int countChannelsBetweenGroupVertices(QosGroupVertex source,
			QosGroupVertex target, DistributionPattern distPattern) {
		int channelCount;
		if (distPattern == DistributionPattern.BIPARTITE) {
			channelCount = source.getNumberOfMembers()
					* target.getNumberOfMembers();
		} else {
			channelCount = Math.max(source.getNumberOfMembers(),
					target.getNumberOfMembers());
		}
		return channelCount;
	}

	private int updateMinChannelCount(
			HashMap<JobVertexID, Integer> channelCounts,
			JobVertexID jobVertexID, int channelCount) {

		int channelCountToSet = channelCount;
		if (channelCounts.containsKey(jobVertexID)) {
			channelCountToSet = Math.min(channelCounts.get(jobVertexID),
					channelCountToSet);
		}
		channelCounts.put(jobVertexID, channelCountToSet);
		return channelCountToSet;
	}

	private void retainCandidatesWithMaxInstanceCount(
			Set<JobVertexID> anchorCandidates, QosGraph qosGraph) {

		int maxInstanceCount = -1;
		for (JobVertexID candidate : anchorCandidates) {
			maxInstanceCount = Math.max(maxInstanceCount, qosGraph
					.getGroupVertexByID(candidate)
					.getNumberOfExecutingInstances());
		}

		Iterator<JobVertexID> candidateIter = anchorCandidates.iterator();
		while (candidateIter.hasNext()) {
			JobVertexID curr = candidateIter.next();
			if (qosGraph.getGroupVertexByID(curr)
					.getNumberOfExecutingInstances() < maxInstanceCount) {
				candidateIter.remove();
			}
		}
	}

	private Set<JobVertexID> collectAnchorCandidates(QosGraph qosGraph) {
		if (qosGraph.getConstraints().size() != 1) {
			throw new RuntimeException(
					"This method can only find the anchor vertex for a single constraint in a QosGraph.");
		}

		Set<JobVertexID> anchorCandidates = new HashSet<JobVertexID>();
		for (SequenceElement<JobVertexID> sequenceElem : qosGraph
				.getConstraints().iterator().next().getSequence()) {

			if (sequenceElem.isVertex()) {
				anchorCandidates.add(sequenceElem.getVertexID());
			} else {
				anchorCandidates.add(sequenceElem.getSourceVertexID());
				anchorCandidates.add(sequenceElem.getTargetVertexID());
			}
		}
		return anchorCandidates;
	}
}
