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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.util.SparseDelegateIterable;

/**
 * @author Bjoern Lohrmann
 */
public class QosGraph {
	
	private QosGraphID qosGraphID;

	private HashSet<QosGroupVertex> startVertices;

	private HashSet<QosGroupVertex> endVertices;

	private HashMap<LatencyConstraintID, JobGraphLatencyConstraint> constraints;

	private HashMap<JobVertexID, QosGroupVertex> vertexByID;

	public QosGraph() {
		this.qosGraphID = new QosGraphID();
		this.startVertices = new HashSet<QosGroupVertex>();
		this.endVertices = new HashSet<QosGroupVertex>();
		this.constraints = new HashMap<LatencyConstraintID, JobGraphLatencyConstraint>();
		this.vertexByID = new HashMap<JobVertexID, QosGroupVertex>();
	}

	public QosGraph(QosGroupVertex startVertex) {
		this();
		this.startVertices.add(startVertex);
		exploreForward(startVertex);
	}

	public QosGraph(QosGroupVertex startVertex,
			JobGraphLatencyConstraint constraint) {
		this(startVertex);
		addConstraint(constraint);
	}

	private void exploreForward(QosGroupVertex vertex) {
		this.vertexByID.put(vertex.getJobVertexID(), vertex);
		for (QosGroupEdge forwardEdge : vertex.getForwardEdges()) {
			exploreForward(forwardEdge.getTargetVertex());
		}

		if (vertex.getNumberOfOutputGates() == 0) {
			this.endVertices.add(vertex);
		}
	}

	/**
	 * Clones and adds all vertices/edges that can be reached via forward
	 * movement from the given vertex into this graph.
	 */
	public void mergeForwardReachableVertices(QosGroupVertex templateVertex) {
		QosGroupVertex vertex = getOrCreate(templateVertex);

		for (QosGroupEdge templateEdge : templateVertex.getForwardEdges()) {
			if (!vertex.hasOutputGate(templateEdge.getOutputGateIndex())) {
				QosGroupVertex edgeTarget = getOrCreate(templateEdge
						.getTargetVertex());
				wireUsingTemplate(vertex, edgeTarget, templateEdge);
				
				// remove vertex from end vertices if necessary (vertex has an
				// output gate now)
				this.endVertices.remove(vertex);

				// remove edgeTarget from start vertices if necessary
				// (edgeTarget has an input gate now)
				this.startVertices.remove(edgeTarget);
			}
			// recursive call
			mergeForwardReachableVertices(templateEdge.getTargetVertex());
		}

		if (vertex.getNumberOfInputGates() == 0) {
			this.startVertices.add(vertex);
		}

		if (vertex.getNumberOfOutputGates() == 0) {
			this.endVertices.add(vertex);
		}
	}

	private void wireUsingTemplate(QosGroupVertex from, QosGroupVertex to,
			QosGroupEdge templateGroupEdge) {

		int outputGateIndex = templateGroupEdge.getOutputGateIndex();
		int inputGateIndex = templateGroupEdge.getInputGateIndex();

		@SuppressWarnings("unused")
		QosGroupEdge clonedGroupEdge = new QosGroupEdge(
				templateGroupEdge.getDistributionPattern(), from, to,
				outputGateIndex, inputGateIndex);

		for (int i = 0; i < from.getNumberOfMembers(); i++) {
			QosVertex fromMember = from.getMember(i);
			QosVertex templateMember = templateGroupEdge.getSourceVertex()
					.getMember(i);

			QosGate outputGate = fromMember.getOutputGate(outputGateIndex);
			if (outputGate == null) {
				outputGate = new QosGate(fromMember, outputGateIndex);
				fromMember.setOutputGate(outputGate);
			}
			QosGate templateOutputGate = templateMember
					.getOutputGate(outputGateIndex);

			addEdgesToOutputGate(outputGate, templateOutputGate, to,
					inputGateIndex);
		}
	}

	private void addEdgesToOutputGate(QosGate outputGate,
			QosGate templateOutputGate, QosGroupVertex to, int inputGateIndex) {
		
		for (QosEdge templateEdge : templateOutputGate.getEdges()) {
			QosEdge clonedEdge = templateEdge.cloneWithoutGates();
			clonedEdge.setOutputGate(outputGate);

			QosVertex toMember = to.getMember(templateEdge.getInputGate()
					.getVertex().getMemberIndex());
			
			QosGate inputGate = toMember.getInputGate(inputGateIndex);
			if (inputGate == null) {
				inputGate = new QosGate(toMember, inputGateIndex);
				toMember.setInputGate(inputGate);
			}

			clonedEdge.setInputGate(inputGate);
		}
	}

	private QosGroupVertex getOrCreate(QosGroupVertex cloneTemplate) {
		QosGroupVertex toReturn = this.vertexByID.get(cloneTemplate
				.getJobVertexID());

		if (toReturn == null) {
			toReturn = cloneTemplate.cloneWithoutEdges();
			this.vertexByID.put(toReturn.getJobVertexID(), toReturn);
		}

		return toReturn;
	}

	/**
	 * Clones and adds all vertices/edges that can be reached via backwards
	 * movement from the given vertex into this graph.
	 */
	public void mergeBackwardReachableVertices(QosGroupVertex templateVertex) {
		QosGroupVertex vertex = getOrCreate(templateVertex);

		for (QosGroupEdge templateEdge : templateVertex.getBackwardEdges()) {
			if (!vertex.hasInputGate(templateEdge.getInputGateIndex())) {
				QosGroupVertex edgeSource = getOrCreate(templateEdge
						.getSourceVertex());
				wireUsingTemplate(edgeSource, vertex, templateEdge);

				// remove edgeSource from end vertices if necessary (edgeSource has an
				// output gate now)
				this.endVertices.remove(edgeSource);

				// remove vertex from start vertices if necessary
				// (vertex has an input gate now)
				this.startVertices.remove(vertex);
			}
			// recursive call
			mergeBackwardReachableVertices(templateEdge.getSourceVertex());
		}

		if (vertex.getNumberOfInputGates() == 0) {
			this.startVertices.add(vertex);
		}

		if (vertex.getNumberOfOutputGates() == 0) {
			this.endVertices.add(vertex);
		}
	}

	public void addConstraint(JobGraphLatencyConstraint constraint) {
		ensureGraphContainsConstrainedVertices(constraint);
		this.constraints.put(constraint.getID(), constraint);
	}

	private void ensureGraphContainsConstrainedVertices(
			JobGraphLatencyConstraint constraint) {
		for (SequenceElement<JobVertexID> seqElem : constraint.getSequence()) {
			if (seqElem.isEdge()) {
				if (!this.vertexByID.containsKey(seqElem.getSourceVertexID())
						|| !this.vertexByID.containsKey(seqElem
								.getTargetVertexID())) {
					throw new IllegalArgumentException(
							"Cannot add constraint to a graph that does not contain the constraint's vertices. This is a bug.");
				}
			}
		}
	}

	public Collection<JobGraphLatencyConstraint> getConstraints() {
		return this.constraints.values();
	}
	
	public JobGraphLatencyConstraint getConstraintByID(LatencyConstraintID constraintID) {
		return this.constraints.get(constraintID);
	}

	// public Iterable<QosGroupVertex> getConstrainedGroupVertices(
	// LatencyConstraintID constraintID) {
	//
	// final JobGraphLatencyConstraint constraint = this.constraints
	// .get(constraintID);
	//
	// return new SparseDelegateIterable<QosGroupVertex>(
	// new Iterator<QosGroupVertex>() {
	// Iterator<SequenceElement<JobVertexID>> sequenceIter = constraint
	// .getSequence().iterator();
	// QosGroupVertex current = null;
	// QosGroupVertex last = null;
	//
	// @Override
	// public boolean hasNext() {
	// while (this.sequenceIter.hasNext()
	// && this.current == null) {
	// SequenceElement<JobVertexID> seqElem = this.sequenceIter
	// .next();
	// if (!seqElem.isVertex()) {
	// QosGroupVertex sourceVertex = QosGraph.this.vertexByID
	// .get(seqElem.getSourceVertexID());
	//
	// if (this.last == sourceVertex) {
	// this.current = QosGraph.this.vertexByID
	// .get(seqElem.getTargetVertexID());
	// ;
	// } else {
	// this.current = sourceVertex;
	// }
	// }
	// }
	//
	// return this.current != null;
	// }
	//
	// @Override
	// public QosGroupVertex next() {
	// this.last = this.current;
	// this.current = null;
	// return this.last;
	// }
	//
	// @Override
	// public void remove() {
	// }
	// });
	// }

	/**
	 * Adds all vertices, edges and constraints of the given graph into this
	 * one.
	 */
	public void merge(QosGraph graph) {
		mergeVerticesAndEdges(graph);
		recomputeStartVertices(graph);
		recomputeEndVertices(graph);
		this.constraints.putAll(graph.constraints);
	}

	private void recomputeStartVertices(QosGraph mergedGraph) {
		Iterator<QosGroupVertex> startVertexIter = this.startVertices
				.iterator();
		while (startVertexIter.hasNext()) {
			if (startVertexIter.next().getNumberOfInputGates() > 0) {
				startVertexIter.remove();
			}
		}

		startVertexIter = mergedGraph.startVertices.iterator();
		while (startVertexIter.hasNext()) {
			QosGroupVertex vertex = this.vertexByID.get(startVertexIter.next()
					.getJobVertexID());
			if (vertex.getNumberOfInputGates() == 0) {
				this.startVertices.add(vertex);
			}
		}
	}

	private void recomputeEndVertices(QosGraph mergedGraph) {
		Iterator<QosGroupVertex> endVertexIter = this.endVertices.iterator();
		while (endVertexIter.hasNext()) {
			if (endVertexIter.next().getNumberOfOutputGates() > 0) {
				endVertexIter.remove();
			}
		}

		endVertexIter = mergedGraph.endVertices.iterator();
		while (endVertexIter.hasNext()) {
			QosGroupVertex vertex = this.vertexByID.get(endVertexIter.next()
					.getJobVertexID());
			if (vertex.getNumberOfOutputGates() == 0) {
				this.endVertices.add(vertex);
			}
		}
	}

	private void mergeVerticesAndEdges(QosGraph graph) {
		for (QosGroupVertex templateVertex : graph.vertexByID.values()) {
			QosGroupVertex edgeSource = getOrCreate(templateVertex);

			for (QosGroupEdge templateEdge : templateVertex.getForwardEdges()) {
				if (!edgeSource
						.hasOutputGate(templateEdge.getOutputGateIndex())) {
					QosGroupVertex egdeTarget = getOrCreate(templateEdge
							.getTargetVertex());
					wireUsingTemplate(edgeSource, egdeTarget, templateEdge);
				}
			}
		}
	}

	public Set<QosGroupVertex> getStartVertices() {
		return Collections.unmodifiableSet(this.startVertices);
	}

	public Set<QosGroupVertex> getEndVertices() {
		return Collections.unmodifiableSet(this.endVertices);
	}

	public QosGroupVertex getGroupVertexByID(JobVertexID vertexID) {
		return this.vertexByID.get(vertexID);
	}

	public Collection<QosGroupVertex> getAllVertices() {
		return Collections.unmodifiableCollection(this.vertexByID.values());
	}

	public int getNumberOfVertices() {
		return this.vertexByID.size();
	}

	/**
	 * Returns the qosGraphID.
	 * 
	 * @return the qosGraphID
	 */
	public QosGraphID getQosGraphID() {
		return this.qosGraphID;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((qosGraphID == null) ? 0 : qosGraphID.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QosGraph other = (QosGraph) obj;
		if (qosGraphID == null) {
			if (other.qosGraphID != null)
				return false;
		} else if (!qosGraphID.equals(other.qosGraphID))
			return false;
		return true;
	}
}
