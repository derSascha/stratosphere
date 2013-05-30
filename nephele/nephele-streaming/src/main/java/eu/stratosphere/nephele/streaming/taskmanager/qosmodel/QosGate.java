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


/**
 * @author Bjoern Lohrmann
 *
 */
public class QosGate {
	
	private int gateIndex;
	
	private ArrayList<QosEdge> edges;
	
	private QosVertex vertex;
	
	/**
	 * Initializes QosGate.
	 *
	 */
	public QosGate(QosVertex vertex, int gateIndex) {
		this.vertex = vertex;
		this.gateIndex = gateIndex;
		this.edges = new ArrayList<QosEdge>();
	}

	public int getGateIndex() {
		return this.gateIndex;
	}

	public void addEdge(QosEdge edge) {
		this.edges.add(edge);
	}
	
	public ArrayList<QosEdge> getEdges() {
		return this.edges;
	}

	public QosVertex getVertex() {
		return this.vertex;
	}	
}
