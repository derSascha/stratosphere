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

import java.util.List;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

/**
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosManagerRole {

	private LatencyConstraintID constraintID;

	private QosGraph qosGraph;

	private QosGroupVertex anchorVertex;
	
	private List<QosVertex> membersOnInstance;

	/**
	 * Initializes ManagerRole.
	 * 
	 * @param qosGraph
	 * @param constraintID
	 * @param anchorVertex
	 */
	public QosManagerRole(QosGraph qosGraph, LatencyConstraintID constraintID,
			QosGroupVertex anchorVertex, List<QosVertex> membersOnInstance) {

		this.qosGraph = qosGraph;
		this.constraintID = constraintID;
		this.anchorVertex = anchorVertex;
		this.membersOnInstance = membersOnInstance;
	}
	
	public InstanceConnectionInfo getManagerInstance() {
		return this.membersOnInstance.get(0).getExecutingInstance();
	}

	/**
	 * Returns the constraintID.
	 * 
	 * @return the constraintID
	 */
	public LatencyConstraintID getConstraintID() {
		return this.constraintID;
	}

	/**
	 * Sets the constraintID to the specified value.
	 * 
	 * @param constraintID
	 *            the constraintID to set
	 */
	public void setConstraintID(LatencyConstraintID constraintID) {
		if (constraintID == null)
			throw new NullPointerException("constraintID must not be null");

		this.constraintID = constraintID;
	}

	/**
	 * Returns the qosGraph.
	 * 
	 * @return the qosGraph
	 */
	public QosGraph getQosGraph() {
		return this.qosGraph;
	}

	/**
	 * Sets the qosGraph to the specified value.
	 * 
	 * @param qosGraph
	 *            the qosGraph to set
	 */
	public void setQosGraph(QosGraph qosGraph) {
		if (qosGraph == null)
			throw new NullPointerException("qosGraph must not be null");

		this.qosGraph = qosGraph;
	}

	/**
	 * Returns the anchorVertex.
	 * 
	 * @return the anchorVertex
	 */
	public QosGroupVertex getAnchorVertex() {
		return this.anchorVertex;
	}

	/**
	 * Sets the anchorVertex to the specified value.
	 * 
	 * @param anchorVertex
	 *            the anchorVertex to set
	 */
	public void setAnchorVertex(QosGroupVertex anchorVertex) {
		if (anchorVertex == null)
			throw new NullPointerException("anchorVertex must not be null");

		this.anchorVertex = anchorVertex;
	}
	
	/**
	 * Returns the membersOnInstance.
	 * 
	 * @return the membersOnInstance
	 */
	public List<QosVertex> getMembersOnInstance() {
		return this.membersOnInstance;
	}

	/**
	 * Sets the membersOnInstance to the specified value.
	 *
	 * @param membersOnInstance the membersOnInstance to set
	 */
	public void setMembersOnInstance(List<QosVertex> membersOnInstance) {
		if (membersOnInstance == null)
			throw new NullPointerException("membersOnInstance must not be null");
	
		this.membersOnInstance = membersOnInstance;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((anchorVertex == null) ? 0 : anchorVertex.hashCode());
		result = prime * result
				+ ((constraintID == null) ? 0 : constraintID.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
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
		QosManagerRole other = (QosManagerRole) obj;
		if (anchorVertex == null) {
			if (other.anchorVertex != null)
				return false;
		} else if (!anchorVertex.equals(other.anchorVertex))
			return false;
		if (constraintID == null) {
			if (other.constraintID != null)
				return false;
		} else if (!constraintID.equals(other.constraintID))
			return false;
		return true;
	}
}
