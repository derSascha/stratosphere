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
package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;

/**
 * Provides a depth-first way a traversing a QoS graph along a JobGraphSequence.
 * 
 * Instances of this class are not thread-safe.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosGraphTraversal {

	private QosVertex startVertex;
	
	private HashSet<ExecutionVertexID> visitedVertices;

	/**
	 * 
	 * Initializes QosGraphTraversal with the given start-vertex.
	 * 
	 * @param startVertex
	 *            Traversal starts from this vertex.
	 */
	public QosGraphTraversal(QosVertex startVertex) {
		this.startVertex = startVertex;
		this.visitedVertices = new HashSet<ExecutionVertexID>();
	}

	/**
	 * Equal to calling
	 * {@link #traverseGraphForwardAlongSequence(QosGraphTraversalListener, JobGraphSequence, true, true)}
	 * .
	 */
	public void traverseGraphForwardAlongSequence(
			QosGraphTraversalListener listener, JobGraphSequence sequence) {

		traverseGraphForwardAlongSequence(listener, sequence, true, true);
	}

	/**
	 * Depth-first-traverses the QosGraph of the start-vertex, along the given
	 * JobGraphSequence. Traversal starts at the start-vertex and for each
	 * encountered vertex or edge the given listener is called.
	 * 
	 * Corner case: Sequences may start/end with edges. If the start-vertex's
	 * group vertex is not part of the sequence, it must at least be the
	 * source/target of the first/last edge in the sequence. The listener will
	 * not be called for the start-vertex then.
	 * 
	 * @param listener
	 *            A callback that is invoked for each vertex or edge encountered
	 *            during depth first traversal.
	 * @param sequence
	 *            Determines which path to walk.
	 * 
	 * @param includeStartVertex
	 *            Whether the listener should also be called for the
	 *            start-vertex. If its group vertex is not in the sequence, this
	 *            parameter has no effect.
	 * @param visitOnlyOnce
	 *            Whether each vertex of the QoS graph shall only be visited
	 *            once (in a DAG there may be multiple paths between two
	 *            vertices).
	 */
	public void traverseGraphForwardAlongSequence(
			QosGraphTraversalListener listener, JobGraphSequence sequence,
			boolean includeStartVertex, boolean visitOnlyOnce) {

		Deque<SequenceElement<JobVertexID>> afterDeque = getSequenceAfterIncluding(sequence);

		if (afterDeque.isEmpty()) {
			return;
		}

		// do some sanity checking
		SequenceElement<JobVertexID> firstElem = afterDeque.getFirst();
		if (!QosGraphUtil.match(firstElem, this.startVertex)
				&& !QosGraphUtil.isEdgeAndStartsAtVertex(firstElem,
						this.startVertex)) {
			throw new RuntimeException(
					"If the start-vertex is not on the sequence it must at least be the source/target of the first/last edge in the sequence.");
		}

		if (!includeStartVertex
				&& QosGraphUtil.match(afterDeque.getFirst(), this.startVertex)) {
			afterDeque.removeFirst();
		}

		if (afterDeque.isEmpty()) {
			return;
		} else if (afterDeque.getFirst().isVertex()) {
			forwardComputeReporters(this.startVertex, afterDeque, listener, visitOnlyOnce);
		} else {
			QosGate outputGate = this.startVertex.getOutputGate(afterDeque
					.getFirst().getOutputGateIndex());
			for (QosEdge edge : outputGate.getEdges()) {
				forwardComputeReporters(edge, afterDeque, listener, visitOnlyOnce);
			}
		}
		
		this.visitedVertices.clear();
	}

	private void forwardComputeReporters(QosVertex vertex,
			Deque<SequenceElement<JobVertexID>> sequenceDeque,
			QosGraphTraversalListener listener, 
			boolean visitOnlyOnce) {
		
		if (visitOnlyOnce && this.visitedVertices.contains(vertex.getID())) {
			return;
		}

		SequenceElement<JobVertexID> currentElem = sequenceDeque.removeFirst();

		listener.processQosVertex(vertex, currentElem);
		
		if(visitOnlyOnce) {
			this.visitedVertices.add(vertex.getID());
		}

		if (!sequenceDeque.isEmpty()) {
			QosGate outputGate = vertex.getOutputGate(sequenceDeque.getFirst()
					.getOutputGateIndex());
			for (QosEdge edge : outputGate.getEdges()) {
				forwardComputeReporters(edge, sequenceDeque, listener, visitOnlyOnce);
			}
		}

		sequenceDeque.addFirst(currentElem);
	}

	private void forwardComputeReporters(QosEdge edge,
			Deque<SequenceElement<JobVertexID>> sequenceDeque,
			QosGraphTraversalListener listener,
			boolean visitOnlyOnce) {

		SequenceElement<JobVertexID> currentElem = sequenceDeque.removeFirst();

		listener.processQosEdge(edge, currentElem);

		if (!sequenceDeque.isEmpty()) {
			forwardComputeReporters(edge.getInputGate().getVertex(),
					sequenceDeque, listener, visitOnlyOnce);
		}

		sequenceDeque.addFirst(currentElem);
	}

	private LinkedList<SequenceElement<JobVertexID>> getSequenceAfterIncluding(
			JobGraphSequence sequence) {

		boolean notInSequence = QosGraphUtil.isEdgeAndEndsAtVertex(
				sequence.getLast(), this.startVertex);
		if (notInSequence) {
			return new LinkedList<SequenceElement<JobVertexID>>();
		}

		LinkedList<SequenceElement<JobVertexID>> ret = new LinkedList<SequenceElement<JobVertexID>>(
				sequence);
		while (!ret.isEmpty()) {
			SequenceElement<JobVertexID> current = ret.getFirst();

			if (QosGraphUtil.match(current, this.startVertex)
					|| QosGraphUtil.isEdgeAndStartsAtVertex(current,
							this.startVertex)) {
				break;
			}

			ret.removeFirst();
		}

		return ret;
	}
	
	/**
	 * Equal to calling
	 * {@link #traverseGraphBackwardAlongSequence(QosGraphTraversalListener, JobGraphSequence, true, true)}
	 * .
	 */
	public void traverseGraphBackwardAlongSequence(
			QosGraphTraversalListener listener, JobGraphSequence sequence) {

		traverseGraphBackwardAlongSequence(listener, sequence, true, true);
	}

	/**
	 * Depth-first-traverses the QosGraph of the start-vertex in backward
	 * direction, along the given JobGraphSequence. Traversal starts at the
	 * start-vertex and for each encountered vertex or edge the given listener
	 * is called.
	 * 
	 * Corner case: Sequences may start/end with edges. If the start-vertex's
	 * group vertex is not part of the sequence, it must at least be the
	 * source/target of the first/last edge in the sequence. The listener will
	 * not be called for the start-vertex then.
	 * 
	 * @param listener
	 *            A callback that is invoked for each vertex or edge encountered
	 *            during backwards depth first traversal.
	 * @param sequence
	 *            Determines which path to walk backwards.
	 * 
	 * @param includeStartVertex
	 *            Whether the listener should also be called for the
	 *            start-vertex. If its group vertex is not in the sequence, this
	 *            parameter has no effect.
	 * @param visitOnlyOnce
	 *            Whether each vertex of the QoS graph shall only be visited
	 *            once (in a DAG there may be multiple paths between two
	 *            vertices).
	 */
	public void traverseGraphBackwardAlongSequence(
			QosGraphTraversalListener listener, JobGraphSequence sequence,
			boolean includeStartVertex,
			boolean visitOnlyOnce) {

		LinkedList<SequenceElement<JobVertexID>> elemsBefore = getSequenceBeforeIncluding(sequence);

		if (elemsBefore.isEmpty()) {
			return;
		}

		// do some sanity checking
		SequenceElement<JobVertexID> lastElem = elemsBefore.getLast();
		if (!QosGraphUtil.match(lastElem, this.startVertex)
				&& !QosGraphUtil.isEdgeAndEndsAtVertex(lastElem,
						this.startVertex)) {
			throw new RuntimeException(
					"If the start-vertex is not on the sequence it must at least be the source/target of the first/last edge in the sequence.");
		}

		if (!includeStartVertex
				&& QosGraphUtil.match(elemsBefore.getLast(), this.startVertex)) {
			elemsBefore.removeLast();
		}

		if (elemsBefore.isEmpty()) {
			return;
		} else if (elemsBefore.getLast().isVertex()) {
			backwardComputeReporters(this.startVertex, elemsBefore, listener, visitOnlyOnce);
		} else {
			QosGate inputGate = this.startVertex.getInputGate(elemsBefore
					.getLast().getInputGateIndex());
			for (QosEdge edge : inputGate.getEdges()) {
				backwardComputeReporters(edge, elemsBefore, listener, visitOnlyOnce);
			}
		}
		
		this.visitedVertices.clear();
	}

	private LinkedList<SequenceElement<JobVertexID>> getSequenceBeforeIncluding(
			JobGraphSequence sequence) {

		boolean notInSequence = QosGraphUtil.isEdgeAndStartsAtVertex(
				sequence.getFirst(), this.startVertex);
		if (notInSequence) {
			return new LinkedList<SequenceElement<JobVertexID>>();
		}

		LinkedList<SequenceElement<JobVertexID>> ret = new LinkedList<SequenceElement<JobVertexID>>(
				sequence);

		while (!ret.isEmpty()) {
			SequenceElement<JobVertexID> current = ret.getLast();

			if (QosGraphUtil.match(current, this.startVertex)
					|| QosGraphUtil.isEdgeAndEndsAtVertex(current,
							this.startVertex)) {
				break;
			}

			ret.removeLast();
		}

		return ret;
	}

	private void backwardComputeReporters(QosVertex vertex,
			Deque<SequenceElement<JobVertexID>> sequenceDeque,
			QosGraphTraversalListener listener,
			boolean visitOnlyOnce) {

		if (visitOnlyOnce && this.visitedVertices.contains(vertex.getID())) {
			return;
		}

		SequenceElement<JobVertexID> currentElem = sequenceDeque.removeLast();

		listener.processQosVertex(vertex, currentElem);
		
		if(visitOnlyOnce) {
			this.visitedVertices.add(vertex.getID());
		}

		if (!sequenceDeque.isEmpty()) {
			QosGate inputGate = vertex.getInputGate(sequenceDeque.getLast()
					.getInputGateIndex());
			for (QosEdge edge : inputGate.getEdges()) {
				backwardComputeReporters(edge, sequenceDeque, listener, visitOnlyOnce);
			}
		}

		sequenceDeque.addLast(currentElem);
	}

	private void backwardComputeReporters(QosEdge edge,
			Deque<SequenceElement<JobVertexID>> sequenceDeque,
			QosGraphTraversalListener listener,
			boolean visitOnlyOnce) {

		SequenceElement<JobVertexID> currentElem = sequenceDeque.removeLast();

		listener.processQosEdge(edge, currentElem);

		if (!sequenceDeque.isEmpty()) {
			backwardComputeReporters(edge.getOutputGate().getVertex(),
					sequenceDeque, listener, visitOnlyOnce);
		}

		sequenceDeque.addLast(currentElem);
	}
}
