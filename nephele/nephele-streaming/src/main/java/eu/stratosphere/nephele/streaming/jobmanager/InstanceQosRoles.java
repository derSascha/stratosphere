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

import java.util.Collection;
import java.util.HashMap;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.jobmanager.QosReporterRole.ReportingAction;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosRolesAction;
import eu.stratosphere.nephele.streaming.message.action.EdgeQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.action.QosManagerConfig;
import eu.stratosphere.nephele.streaming.message.action.VertexQosReporterConfig;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class InstanceQosRoles {

	private InstanceConnectionInfo connectionInfo;

	private HashMap<LatencyConstraintID, QosManagerRole> managerRoles;

	private HashMap<QosReporterID, QosReporterRole> reporterRoles;

	public InstanceQosRoles(InstanceConnectionInfo connectionInfo) {
		this.connectionInfo = connectionInfo;
		this.managerRoles = new HashMap<LatencyConstraintID, QosManagerRole>();
		this.reporterRoles = new HashMap<QosReporterID, QosReporterRole>();
	}

	public InstanceConnectionInfo getConnectionInfo() {
		return this.connectionInfo;
	}

	public void addManagerRole(QosManagerRole managerRole) {
		if (this.managerRoles.containsKey(managerRole.getConstraintID())) {
			throw new RuntimeException(
					"QoS Manager role for this constraint has already been defined");
		}

		this.managerRoles.put(managerRole.getConstraintID(), managerRole);
	}

	public void addReporterRole(QosReporterRole reporterRole) {
		QosReporterID reporterID = reporterRole.getReporterID();
		if (this.reporterRoles.containsKey(reporterID)) {
			this.reporterRoles.get(reporterID).mergeInto(reporterRole);
		} else {
			this.reporterRoles.put(reporterID, reporterRole);
		}
	}

	public Collection<QosManagerRole> getManagerRoles() {
		return this.managerRoles.values();
	}

	public DeployInstanceQosRolesAction toDeploymentAction(JobID jobID) {
		DeployInstanceQosRolesAction deploymentAction = new DeployInstanceQosRolesAction(
				jobID, this.connectionInfo);

		if (!this.managerRoles.isEmpty()) {
			addQosManagerConfig(deploymentAction);
		}

		for (QosReporterRole reporterRole : this.reporterRoles.values()) {
			if (reporterRole.getAction() == ReportingAction.REPORT_CHANNEL_STATS) {
				deploymentAction
						.addEdgeQosReporter(toEdgeQosReporterConfig(reporterRole));
			} else {
				deploymentAction
						.addVertexQosReporter(toVertexQosReporterConfig(reporterRole));
			}
		}

		return deploymentAction;
	}

	private void addQosManagerConfig(
			DeployInstanceQosRolesAction deploymentAction) {
		
		QosGraph shallowQosGraph = null;
		for (QosManagerRole managerRole : this.managerRoles.values()) {
			if (shallowQosGraph == null) {
				shallowQosGraph = managerRole.getQosGraph()
						.cloneWithoutMembers();
			} else {
				shallowQosGraph.merge(managerRole.getQosGraph()
						.cloneWithoutMembers());
			}
		}
		deploymentAction.setQosManager(new QosManagerConfig(shallowQosGraph));
	}

	private VertexQosReporterConfig toVertexQosReporterConfig(
			QosReporterRole reporterRole) {

		QosVertex vertex = reporterRole.getVertex();

		InstanceConnectionInfo[] managers = reporterRole.getTargetQosManagers()
				.toArray(new InstanceConnectionInfo[0]);

		int inputGateIndex = reporterRole.getInputGateIndex();
		int outputGateIndex = reporterRole.getOutputGateIndex();

		VertexQosReporterConfig vertexReporter = new VertexQosReporterConfig(
				vertex.getGroupVertex().getJobVertexID(), vertex.getID(),
				vertex.getExecutingInstance(), managers, inputGateIndex,
				(inputGateIndex != -1) ? vertex.getInputGate(inputGateIndex)
						.getGateID() : null, outputGateIndex,
				(outputGateIndex != -1) ? vertex.getOutputGate(outputGateIndex)
						.getGateID() : null, vertex.getMemberIndex(),
				vertex.getName());

		return vertexReporter;
	}

	private EdgeQosReporterConfig toEdgeQosReporterConfig(
			QosReporterRole reporterRole) {

		QosEdge edge = reporterRole.getEdge();

		InstanceConnectionInfo[] managers = reporterRole.getTargetQosManagers()
				.toArray(new InstanceConnectionInfo[0]);

		EdgeQosReporterConfig edgeReporter = new EdgeQosReporterConfig(
				edge.getSourceChannelID(), edge.getTargetChannelID(), managers,
				edge.getOutputGate().getGateID(), edge.getInputGate()
						.getGateID(), edge.getOutputGateEdgeIndex(),
				edge.getInputGateEdgeIndex(), edge.toString());

		return edgeReporter;
	}
}
