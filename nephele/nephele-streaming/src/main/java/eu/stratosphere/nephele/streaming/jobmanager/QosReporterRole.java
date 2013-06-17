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

import java.util.Set;
import java.util.TreeSet;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class QosReporterRole {

	public enum ReportingAction {
		REPORT_TASK_STATS, REPORT_CHANNEL_STATS
	};

	private TreeSet<InstanceConnectionInfo> targetQosManagers = new TreeSet<InstanceConnectionInfo>();

	private ReportingAction action;

	private QosVertex vertex;

	private int inputGateIndex;

	private int outputGateIndex;

	private QosEdge edge;
	
	public class ReporterRoleID {
		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			return QosReporterRole.this.hashCode();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			return QosReporterRole.this.equals(obj);
		}
	}

	public QosReporterRole(QosVertex vertex, int inputGateIndex,
			int outputGateIndex, InstanceConnectionInfo targetQosManager) {

		this.action = ReportingAction.REPORT_TASK_STATS;
		this.vertex = vertex;
		this.targetQosManagers.add(targetQosManager);
		this.inputGateIndex = inputGateIndex;
		this.outputGateIndex = outputGateIndex;
	}

	public QosReporterRole(QosEdge edge, InstanceConnectionInfo targetQosManager) {

		this.action = ReportingAction.REPORT_CHANNEL_STATS;
		this.edge = edge;
		this.targetQosManagers.add(targetQosManager);
	}

	public void mergeInto(QosReporterRole otherRole) {
		if (!this.equals(otherRole)) {
			throw new RuntimeException("Cannot merge unequal QosReporter roles");
		}
		this.targetQosManagers.addAll(otherRole.targetQosManagers);
	}

	/**
	 * Returns the targetQosManagers.
	 * 
	 * @return the targetQosManagers
	 */
	public Set<InstanceConnectionInfo> getTargetQosManager() {
		return this.targetQosManagers;
	}

	/**
	 * Returns the action.
	 * 
	 * @return the action
	 */
	public ReportingAction getAction() {
		return this.action;
	}

	/**
	 * Returns the vertex.
	 * 
	 * @return the vertex
	 */
	public QosVertex getVertex() {
		return this.vertex;
	}

	/**
	 * Returns the inputGateIndex.
	 * 
	 * @return the inputGateIndex
	 */
	public int getInputGateIndex() {
		return this.inputGateIndex;
	}

	/**
	 * Returns the outputGateIndex.
	 * 
	 * @return the outputGateIndex
	 */
	public int getOutputGateIndex() {
		return this.outputGateIndex;
	}

	/**
	 * Returns the edge.
	 * 
	 * @return the edge
	 */
	public QosEdge getEdge() {
		return this.edge;
	}
	
	public ReporterRoleID getReporterRoleID() {
		return new ReporterRoleID();
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
		result = prime * result + ((edge == null) ? 0 : edge.hashCode());
		result = prime * result + inputGateIndex;
		result = prime * result + outputGateIndex;
		result = prime * result + ((vertex == null) ? 0 : vertex.hashCode());
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
		QosReporterRole other = (QosReporterRole) obj;
		if (edge == null) {
			if (other.edge != null)
				return false;
		} else if (!edge.equals(other.edge))
			return false;
		if (inputGateIndex != other.inputGateIndex)
			return false;
		if (outputGateIndex != other.outputGateIndex)
			return false;
		if (vertex == null) {
			if (other.vertex != null)
				return false;
		} else if (!vertex.equals(other.vertex))
			return false;
		return true;
	}
}
