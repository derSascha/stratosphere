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

import java.util.ArrayList;

import eu.stratosphere.nephele.streaming.util.SparseDelegateIterable;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class QosGate {

	private int gateIndex;

	/**
	 * Sparse list of edges, which means this list may contain null entries.
	 */
	private ArrayList<QosEdge> edges;

	private QosVertex vertex;

	public enum GateType {
		INPUT_GATE, OUTPUT_GATE;
	}

	private GateType isOutputGate;

	private int noOfEdges;

	/**
	 * Initializes QosGate.
	 * 
	 */
	public QosGate(int gateIndex) {
		this.gateIndex = gateIndex;
		this.edges = new ArrayList<QosEdge>();
		this.noOfEdges = 0;
	}

	public int getGateIndex() {
		return this.gateIndex;
	}

	public void setVertex(QosVertex vertex) {
		this.vertex = vertex;
	}

	public void setGateType(GateType gateType) {
		this.isOutputGate = gateType;
	}

	public void addEdge(QosEdge edge) {
		int edgeIndex = (this.isOutputGate == GateType.OUTPUT_GATE) ? edge
				.getOutputGateEdgeIndex() : edge.getInputGateEdgeIndex();

		if (edgeIndex >= this.edges.size()) {
			fillWithNulls(this.edges, edgeIndex + 1);
		}

		if (this.edges.get(edgeIndex) == null) {
			this.noOfEdges++;
		}
		this.edges.set(edgeIndex, edge);
	}

	private <T> void fillWithNulls(ArrayList<T> list, int targetSize) {
		int toAdd = targetSize - list.size();

		for (int i = 0; i < toAdd; i++) {
			list.add(null);
		}
	}

	public SparseDelegateIterable<QosEdge> getEdges() {
		return new SparseDelegateIterable<QosEdge>(this.edges.iterator());
	}

	public QosEdge getEdge(int edgeIndex) {
		QosEdge toReturn = null;

		if (this.edges.size() > edgeIndex) {
			toReturn = this.edges.get(edgeIndex);
		}

		return toReturn;
	}

	public int getNumberOfEdges() {
		return this.noOfEdges;
	}

	public QosVertex getVertex() {
		return this.vertex;
	}
}
