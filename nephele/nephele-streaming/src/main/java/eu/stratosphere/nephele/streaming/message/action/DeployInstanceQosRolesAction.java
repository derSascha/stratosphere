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
import java.util.LinkedList;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.AbstractStreamMessage;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class DeployInstanceQosRolesAction extends AbstractStreamMessage {

	private QosManagerDeploymentDescriptor qosManager = new QosManagerDeploymentDescriptor();

	private LinkedList<EdgeQosReporterDeploymentDescriptor> edgeQosReporters = new LinkedList<EdgeQosReporterDeploymentDescriptor>();

	private LinkedList<VertexQosReporterDeploymentDescriptor> vertexQosReporters = new LinkedList<VertexQosReporterDeploymentDescriptor>();

	public DeployInstanceQosRolesAction() {
	}

	public DeployInstanceQosRolesAction(JobID jobID) {
		super(jobID);
	}

	public void setQosManager(QosManagerDeploymentDescriptor qosManager) {
		this.qosManager = qosManager;
	}

	public void addEdgeQosReporter(
			EdgeQosReporterDeploymentDescriptor edgeQosReporter) {
		this.edgeQosReporters.add(edgeQosReporter);
	}

	public void addVertexQosReporter(
			VertexQosReporterDeploymentDescriptor vertexQosReporter) {
		this.vertexQosReporters.add(vertexQosReporter);
	}

	public QosManagerDeploymentDescriptor getQosManager() {
		return this.qosManager;
	}

	public LinkedList<EdgeQosReporterDeploymentDescriptor> getEdgeQosReporters() {
		return this.edgeQosReporters;
	}

	public LinkedList<VertexQosReporterDeploymentDescriptor> getVertexQosReporters() {
		return this.vertexQosReporters;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		out.writeBoolean(this.qosManager != null);
		if (this.qosManager != null) {
			this.qosManager.write(out);
		}
		out.writeInt(this.edgeQosReporters.size());
		for (EdgeQosReporterDeploymentDescriptor edgeQosReporter : this.edgeQosReporters) {
			edgeQosReporter.write(out);
		}
		out.writeInt(this.vertexQosReporters.size());
		for (VertexQosReporterDeploymentDescriptor vertexQosReporter : this.vertexQosReporters) {
			vertexQosReporter.write(out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		if (in.readBoolean()) {
			this.qosManager = new QosManagerDeploymentDescriptor();
			this.qosManager.read(in);
		}

		int noOfEdgeQosReporters = in.readInt();
		for (int i = 0; i < noOfEdgeQosReporters; i++) {
			EdgeQosReporterDeploymentDescriptor edgeQosReporter = new EdgeQosReporterDeploymentDescriptor();
			edgeQosReporter.read(in);
			this.edgeQosReporters.add(edgeQosReporter);
		}

		int noOfVertexQosReporters = in.readInt();
		for (int i = 0; i < noOfVertexQosReporters; i++) {
			VertexQosReporterDeploymentDescriptor vertexQosReporter = new VertexQosReporterDeploymentDescriptor();
			vertexQosReporter.read(in);
			this.vertexQosReporters.add(vertexQosReporter);
		}
	}
}
