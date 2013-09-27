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

import java.util.LinkedList;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGate;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphTraversal;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphTraversalListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class CandidateChainFinder {

	private QosGraphTraversalListener traversalListener = new QosGraphTraversalListener() {

		public InstanceConnectionInfo executingTaskManager = null;

		public LinkedList<ExecutionVertexID> currentChain = new LinkedList<ExecutionVertexID>();

		public int lastChainElementSequenceIndex = -1;

		@Override
		public void processQosVertex(QosVertex vertex,
				SequenceElement<JobVertexID> sequenceElem) {

			if (this.currentChain.isEmpty()) {
				tryToStartChain(vertex, sequenceElem);
			} else {
				appendToOrRestartChain(vertex, sequenceElem);
			}
		}

		private void appendToOrRestartChain(QosVertex vertex,
				SequenceElement<JobVertexID> sequenceElem) {

			int noOfInputGatesInExecutionGraph = CandidateChainFinder.this.executionGraph
					.getVertexByID(vertex.getID()).getNumberOfInputGates();

			int noOfChannelsOnInputGate = vertex.getInputGate(
					sequenceElem.getInputGateIndex()).getNumberOfEdges();

			if (noOfInputGatesInExecutionGraph == 1
					&& noOfChannelsOnInputGate == 1
					&& vertex.getExecutingInstance().equals(
							this.executingTaskManager)
					&& this.lastChainElementSequenceIndex < sequenceElem
							.getIndexInSequence()) {

				this.currentChain.add(vertex.getID());
				this.lastChainElementSequenceIndex = sequenceElem
						.getIndexInSequence();
			} else {
				finishChain();
				tryToStartChain(vertex, sequenceElem);
			}
		}

		private void tryToStartChain(QosVertex vertex,
				SequenceElement<JobVertexID> sequenceElem) {

			QosGate outputGate = vertex.getOutputGate(sequenceElem
					.getOutputGateIndex());

			int noOfEdgesInOutputGate = outputGate.getNumberOfEdges();

			int noOfOutputGatesInExecutionGraph = CandidateChainFinder.this.executionGraph
					.getVertexByID(vertex.getID()).getNumberOfOutputGates();

			if (noOfEdgesInOutputGate == 1
					&& noOfOutputGatesInExecutionGraph == 1) {
				this.executingTaskManager = vertex.getExecutingInstance();
				this.currentChain.add(outputGate.getVertex().getID());
				this.lastChainElementSequenceIndex = sequenceElem
						.getIndexInSequence();
			}
		}

		@Override
		public void processQosEdge(QosEdge edge,
				SequenceElement<JobVertexID> sequenceElem) {
			// do nothing
		}

		private void finishChain() {
			if (this.currentChain.size() >= 2) {
				CandidateChainFinder.this.chainListener.handleCandidateChain(
						this.executingTaskManager, this.currentChain);
			}

			this.executingTaskManager = null;
			this.lastChainElementSequenceIndex = -1;
			this.currentChain.clear();
		}
	};

	private QosGraphTraversal traversal;

	private CandidateChainListener chainListener;

	private ExecutionGraph executionGraph;

	public CandidateChainFinder(CandidateChainListener chainListener,
			ExecutionGraph executionGraph) {
		this.executionGraph = executionGraph;
		this.chainListener = chainListener;
		this.traversal = new QosGraphTraversal(null, null,
				this.traversalListener);
		this.traversal.setClearTraversedVertices(false);
	}

	public void findChainsAlongConstraint(LatencyConstraintID constraintID,
			QosGraph qosGraph) {

		this.traversal.setSequence(qosGraph.getConstraintByID(constraintID)
				.getSequence());

		for (QosGroupVertex groupStartVertex : qosGraph.getStartVertices()) {
			for (QosVertex startVertex : groupStartVertex.getMembers()) {
				this.traversal.setStartVertex(startVertex);
				this.traversal.traverseForward();
			}
		}
	}
}
