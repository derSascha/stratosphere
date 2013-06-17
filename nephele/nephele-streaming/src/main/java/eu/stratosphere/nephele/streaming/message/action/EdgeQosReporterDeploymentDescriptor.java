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
package eu.stratosphere.nephele.streaming.message.action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class EdgeQosReporterDeploymentDescriptor implements IOReadableWritable {

	private ChannelID sourceChannelID;

	private ChannelID targetChannelID;

	private InstanceConnectionInfo[] qosManagers;
	
	private int outputGateIndex;
	
	private int inputGateIndex;

	private int outputGateEdgeIndex;

	private int inputGateEdgeIndex;
	
	
	public EdgeQosReporterDeploymentDescriptor() {
	}
	

	/**
	 * Initializes EdgeQosReporterDeploymentDescriptor.
	 *
	 * @param sourceChannelID
	 * @param targetChannelID
	 * @param qosManagers
	 * @param action
	 * @param outputGateIndex
	 * @param inputGateIndex
	 * @param outputGateEdgeIndex
	 * @param inputGateEdgeIndex
	 */
	public EdgeQosReporterDeploymentDescriptor(ChannelID sourceChannelID,
			ChannelID targetChannelID, InstanceConnectionInfo[] qosManagers,
			int outputGateIndex, int inputGateIndex,
			int outputGateEdgeIndex, int inputGateEdgeIndex) {
		
		this.sourceChannelID = sourceChannelID;
		this.targetChannelID = targetChannelID;
		this.qosManagers = qosManagers;
		this.outputGateIndex = outputGateIndex;
		this.inputGateIndex = inputGateIndex;
		this.outputGateEdgeIndex = outputGateEdgeIndex;
		this.inputGateEdgeIndex = inputGateEdgeIndex;
	}
	
	

	/**
	 * Returns the sourceChannelID.
	 * 
	 * @return the sourceChannelID
	 */
	public ChannelID getSourceChannelID() {
		return this.sourceChannelID;
	}


	/**
	 * Returns the targetChannelID.
	 * 
	 * @return the targetChannelID
	 */
	public ChannelID getTargetChannelID() {
		return this.targetChannelID;
	}


	/**
	 * Returns the qosManagers.
	 * 
	 * @return the qosManagers
	 */
	public InstanceConnectionInfo[] getQosManagers() {
		return this.qosManagers;
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
	 * Returns the inputGateIndex.
	 * 
	 * @return the inputGateIndex
	 */
	public int getInputGateIndex() {
		return this.inputGateIndex;
	}


	/**
	 * Returns the outputGateEdgeIndex.
	 * 
	 * @return the outputGateEdgeIndex
	 */
	public int getOutputGateEdgeIndex() {
		return this.outputGateEdgeIndex;
	}


	/**
	 * Returns the inputGateEdgeIndex.
	 * 
	 * @return the inputGateEdgeIndex
	 */
	public int getInputGateEdgeIndex() {
		return this.inputGateEdgeIndex;
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.sourceChannelID.write(out);
		this.targetChannelID.write(out);
		out.writeInt(this.qosManagers.length);
		for (InstanceConnectionInfo qosManager : this.qosManagers) {
			qosManager.write(out);
		}
		out.writeInt(this.outputGateIndex);
		out.writeInt(this.inputGateIndex);
		out.writeInt(this.outputGateEdgeIndex);
		out.writeInt(this.inputGateEdgeIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		this.sourceChannelID = new ChannelID();
		this.sourceChannelID.read(in);
		this.targetChannelID = new ChannelID();
		this.targetChannelID.read(in);
		this.qosManagers = new InstanceConnectionInfo[in.readInt()];
		for (int i = 0; i < this.qosManagers.length; i++) {
			this.qosManagers[i] = new InstanceConnectionInfo();
			this.qosManagers[i].read(in);
		}
		this.outputGateIndex = in.readInt();
		this.inputGateIndex = in.readInt();
		this.outputGateEdgeIndex = in.readInt();
		this.inputGateEdgeIndex = in.readInt();
	}
}
