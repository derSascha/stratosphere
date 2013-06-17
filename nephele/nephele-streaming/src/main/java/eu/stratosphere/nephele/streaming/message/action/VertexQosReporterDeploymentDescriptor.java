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

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobVertexID;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class VertexQosReporterDeploymentDescriptor implements
		IOReadableWritable {

	private JobVertexID groupVertexID;

	private ExecutionVertexID vertexID;

	private InstanceConnectionInfo[] qosManagers;

	private int inputGateIndex;

	private int outputGateIndex;

	private int memberIndex;

	private String name;
	
	public VertexQosReporterDeploymentDescriptor() {
	}

	/**
	 * Initializes VertexQosReporterDeploymentDescriptor.
	 *
	 * @param groupVertexID
	 * @param vertexID
	 * @param qosManagers
	 * @param inputGateIndex
	 * @param outputGateIndex
	 * @param memberIndex
	 * @param name
	 */
	public VertexQosReporterDeploymentDescriptor(JobVertexID groupVertexID,
			ExecutionVertexID vertexID, InstanceConnectionInfo[] qosManagers,
			int inputGateIndex, int outputGateIndex, int memberIndex,
			String name) {
		this.groupVertexID = groupVertexID;
		this.vertexID = vertexID;
		this.qosManagers = qosManagers;
		this.inputGateIndex = inputGateIndex;
		this.outputGateIndex = outputGateIndex;
		this.memberIndex = memberIndex;
		this.name = name;
	}
	
	

	/**
	 * Returns the groupVertexID.
	 * 
	 * @return the groupVertexID
	 */
	public JobVertexID getGroupVertexID() {
		return this.groupVertexID;
	}

	/**
	 * Returns the vertexID.
	 * 
	 * @return the vertexID
	 */
	public ExecutionVertexID getVertexID() {
		return this.vertexID;
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
	 * Returns the memberIndex.
	 * 
	 * @return the memberIndex
	 */
	public int getMemberIndex() {
		return this.memberIndex;
	}

	/**
	 * Returns the name.
	 * 
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.groupVertexID.write(out);
		this.vertexID.write(out);
		out.writeInt(this.qosManagers.length);
		for (InstanceConnectionInfo qosManager : this.qosManagers) {
			qosManager.write(out);
		}
		out.writeInt(this.inputGateIndex);
		out.writeInt(this.outputGateIndex);
		out.writeInt(this.memberIndex);
		out.writeUTF(this.name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		this.groupVertexID = new JobVertexID();
		this.groupVertexID.read(in);
		this.vertexID = new ExecutionVertexID();
		this.vertexID.read(in);
		this.qosManagers = new InstanceConnectionInfo[in.readInt()];
		for (int i = 0; i < this.qosManagers.length; i++) {
			this.qosManagers[i] = new InstanceConnectionInfo();
			this.qosManagers[i].read(in);
		}
		this.inputGateIndex = in.readInt();
		this.outputGateIndex = in.readInt();
		this.memberIndex = in.readInt();
		this.name = in.readUTF();
	}

}
