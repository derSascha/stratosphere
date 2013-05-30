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

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class QosReporterRole {

	public enum ReportingAction {
		REPORT_TASK_STATS, REPORT_CHANNEL_STATS	
	};

	private InstanceConnectionInfo targetQosManager;

	private ReportingAction action;

	private ExecutionVertexID executionVertexID;

	private int inputGateIndex;

	private int outputGateIndex;

	private ChannelID channelID;

	public QosReporterRole(ExecutionVertexID executionVertexID,
			int inputGateIndex, int outputGateIndex,
			InstanceConnectionInfo targetQosManager) {

		this.action = ReportingAction.REPORT_TASK_STATS;
		this.targetQosManager = targetQosManager;
		this.executionVertexID = executionVertexID;
		this.inputGateIndex = inputGateIndex;
		this.outputGateIndex = outputGateIndex;
	}

	public QosReporterRole(ChannelID channelID,
			InstanceConnectionInfo targetQosManager) {
		
		this.action = ReportingAction.REPORT_CHANNEL_STATS;
		this.targetQosManager = targetQosManager;
		this.channelID = channelID;
	}

	/**
	 * Returns the targetQosManager.
	 * 
	 * @return the targetQosManager
	 */
	public InstanceConnectionInfo getTargetQosManager() {
		return this.targetQosManager;
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
	 * Returns the executionVertexID.
	 * 
	 * @return the executionVertexID
	 */
	public ExecutionVertexID getExecutionVertexID() {
		return this.executionVertexID;
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
	 * Returns the channelID.
	 * 
	 * @return the channelID
	 */
	public ChannelID getChannelID() {
		return this.channelID;
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
				+ ((channelID == null) ? 0 : channelID.hashCode());
		result = prime
				* result
				+ ((executionVertexID == null) ? 0 : executionVertexID
						.hashCode());
		result = prime * result + inputGateIndex;
		result = prime * result + outputGateIndex;
		result = prime
				* result
				+ ((targetQosManager == null) ? 0 : targetQosManager.hashCode());
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
		if (channelID == null) {
			if (other.channelID != null)
				return false;
		} else if (!channelID.equals(other.channelID))
			return false;
		if (executionVertexID == null) {
			if (other.executionVertexID != null)
				return false;
		} else if (!executionVertexID.equals(other.executionVertexID))
			return false;
		if (inputGateIndex != other.inputGateIndex)
			return false;
		if (outputGateIndex != other.outputGateIndex)
			return false;
		if (targetQosManager == null) {
			if (other.targetQosManager != null)
				return false;
		} else if (!targetQosManager.equals(other.targetQosManager))
			return false;
		return true;
	}
}
