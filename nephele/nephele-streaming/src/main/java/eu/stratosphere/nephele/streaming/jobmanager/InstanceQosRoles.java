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
import eu.stratosphere.nephele.streaming.jobmanager.QosReporterRole.ReporterRoleID;
import eu.stratosphere.nephele.streaming.jobmanager.QosReporterRole.ReportingAction;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosRolesAction;
import eu.stratosphere.nephele.streaming.message.action.EdgeQosReporterDeploymentDescriptor;
import eu.stratosphere.nephele.streaming.message.action.QosManagerDeploymentDescriptor;
import eu.stratosphere.nephele.streaming.message.action.VertexQosReporterDeploymentDescriptor;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class InstanceQosRoles {

	private InstanceConnectionInfo connectionInfo;

	private HashMap<LatencyConstraintID, QosManagerRole> managerRoles;

	private HashMap<ReporterRoleID, QosReporterRole> reporterRoles;

	public InstanceQosRoles(InstanceConnectionInfo connectionInfo) {
		this.connectionInfo = connectionInfo;
		this.managerRoles = new HashMap<LatencyConstraintID, QosManagerRole>();
		this.reporterRoles = new HashMap<ReporterRoleID, QosReporterRole>();
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
		ReporterRoleID reporterRoleID = reporterRole.getReporterRoleID();
		if (this.reporterRoles.containsKey(reporterRoleID)) {
			this.reporterRoles.get(reporterRoleID).mergeInto(reporterRole);
		} else {
			this.reporterRoles.put(reporterRoleID, reporterRole);
		}
	}

	public Collection<QosManagerRole> getManagerRoles() {
		return this.managerRoles.values();
	}

	public DeployInstanceQosRolesAction toDeploymentAction(JobID jobID) {
		DeployInstanceQosRolesAction deploymentAction = new DeployInstanceQosRolesAction(
				jobID);

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
		deploymentAction.setQosManager(new QosManagerDeploymentDescriptor(
				shallowQosGraph));
		for (QosReporterRole reporterRole : this.reporterRoles.values()) {

			if (reporterRole.getAction() == ReportingAction.REPORT_CHANNEL_STATS) {
				deploymentAction
						.addEdgeQosReporter(toEdgeQosReporterDeploymentDescriptor(reporterRole));
			} else {
				deploymentAction
						.addVertexQosReporter(toVertexQosReporterDeploymentDescriptor(reporterRole));
			}
		}

		return deploymentAction;
	}

	private VertexQosReporterDeploymentDescriptor toVertexQosReporterDeploymentDescriptor(
			QosReporterRole reporterRole) {
		QosVertex vertex = reporterRole.getVertex();
		InstanceConnectionInfo[] managers = (InstanceConnectionInfo[]) reporterRole
				.getTargetQosManager().toArray();

		VertexQosReporterDeploymentDescriptor vertexReporter = new VertexQosReporterDeploymentDescriptor(
				vertex.getGroupVertex().getJobVertexID(),
				vertex.getID(), managers,
				reporterRole.getInputGateIndex(),
				reporterRole.getInputGateIndex(),
				vertex.getMemberIndex(), vertex.getName());
		return vertexReporter;
	}

	private EdgeQosReporterDeploymentDescriptor toEdgeQosReporterDeploymentDescriptor(
			QosReporterRole reporterRole) {
		QosEdge edge = reporterRole.getEdge();
		InstanceConnectionInfo[] managers = (InstanceConnectionInfo[]) reporterRole
				.getTargetQosManager().toArray();
		EdgeQosReporterDeploymentDescriptor edgeReporter = new EdgeQosReporterDeploymentDescriptor(
				edge.getSourceChannelID(), edge.getTargetChannelID(),
				managers, edge.getOutputGate().getGateIndex(), edge
						.getInputGate().getGateIndex(),
				edge.getOutputGateEdgeIndex(),
				edge.getInputGateEdgeIndex());
		return edgeReporter;
	}
}
