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
package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.AbstractStreamMessage;
import eu.stratosphere.nephele.streaming.message.action.ConstructStreamChainAction;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosRolesAction;
import eu.stratosphere.nephele.streaming.message.action.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.message.qosreport.QosReport;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.StreamTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosManagerThread;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.StreamChainCoordinator;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;

/**
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class StreamJobEnvironment {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory
			.getLog(StreamJobEnvironment.class);

	private JobID jobID;

	private QosReportForwarderThread qosReportForwarder;

	private QosManagerThread qosManager;

	private StreamChainCoordinator chainCoordinator;

	private HashMap<ExecutionVertexID, StreamTaskQosCoordinator> taskQosCoordinators;

	private StreamMessagingThread messagingThread;

	private boolean environmentIsShutDown;

	public StreamJobEnvironment(JobID jobID,
			StreamMessagingThread messagingThread) {

		this.jobID = jobID;
		this.messagingThread = messagingThread;
		this.environmentIsShutDown = false;

		QosReporterConfigCenter reporterConfig = new QosReporterConfigCenter();
		reporterConfig.setAggregationInterval(StreamTaskManagerPlugin
				.getDefaultAggregationInterval());
		reporterConfig.setTaggingInterval(StreamTaskManagerPlugin
				.getDefaultTaggingInterval());

		this.qosReportForwarder = new QosReportForwarderThread(jobID,
				messagingThread, reporterConfig);

		this.chainCoordinator = new StreamChainCoordinator();
		this.taskQosCoordinators = new HashMap<ExecutionVertexID, StreamTaskQosCoordinator>();
	}

	public JobID getJobID() {
		return this.jobID;
	}

	
	public void registerTask(RuntimeTask task, StreamTaskEnvironment streamEnv) {
		if (this.environmentIsShutDown) {
			return;
		}

		updateAggregationAndTaggingIntervals(streamEnv);

		synchronized (this.taskQosCoordinators) {
			if (this.taskQosCoordinators.containsKey(task.getVertexID())) {
				throw new RuntimeException(String.format(
						"Task %s is already registered",
						streamEnv.getTaskName()));
			}

			this.taskQosCoordinators.put(task.getVertexID(),
					new StreamTaskQosCoordinator(task.getVertexID(), streamEnv,
							this.qosReportForwarder, this.chainCoordinator));
		}
	}

	private void updateAggregationAndTaggingIntervals(Environment taskEnvironment) {
		long aggregationInterval = taskEnvironment
				.getJobConfiguration()
				.getLong(StreamTaskManagerPlugin.AGGREGATION_INTERVAL_KEY,
						StreamTaskManagerPlugin.getDefaultAggregationInterval());
		int taggingInterval = taskEnvironment.getJobConfiguration().getInteger(
				StreamTaskManagerPlugin.TAGGING_INTERVAL_KEY,
				StreamTaskManagerPlugin.getDefaultTaggingInterval());

		this.qosReportForwarder.getConfigCenter().setAggregationInterval(
				aggregationInterval);
		this.qosReportForwarder.getConfigCenter().setTaggingInterval(
				taggingInterval);
	}

	public StreamTaskQosCoordinator getTaskQosCoordinator(
			ExecutionVertexID vertexID) {
		return this.taskQosCoordinators.get(vertexID);
	}

	public void shutdown() {
		this.environmentIsShutDown = true;
		if (this.qosManager != null) {
			this.qosManager.shutdown();
		}
		this.qosManager = null;

		this.qosReportForwarder.shutdown();
		this.qosReportForwarder = null;
	}

	public void handleStreamMessage(AbstractStreamMessage streamMsg) {
		if (this.environmentIsShutDown) {
			return;
		}

		if (streamMsg instanceof QosReport) {
			this.handleQosReport((QosReport) streamMsg);
		} else if (streamMsg instanceof LimitBufferSizeAction) {
			this.handleLimitBufferSizeAction((LimitBufferSizeAction) streamMsg);
		} else if (streamMsg instanceof ConstructStreamChainAction) {
			this.handleConstructStreamChainAction((ConstructStreamChainAction) streamMsg);
		} else if (streamMsg instanceof DeployInstanceQosRolesAction) {
			this.handleDeployInstanceQosRolesAction((DeployInstanceQosRolesAction) streamMsg);
		} else {
			LOG.error("Received message is of unknown type "
					+ streamMsg.getClass());
		}
	}

	private void handleDeployInstanceQosRolesAction(
			DeployInstanceQosRolesAction deployRolesAction) {

		if (deployRolesAction.getQosManager() != null) {
			processQosManagerConfig(deployRolesAction);
		}

		this.qosReportForwarder.configureReporting(deployRolesAction);

		LOG.info(String
				.format("Deployed %d vertex Qos reporters, %d edge Qos reporters and %d Qos manager roles",
						deployRolesAction.getVertexQosReporters().size(),
						deployRolesAction.getEdgeQosReporters().size(),
						(deployRolesAction.getQosManager() != null) ? 1 : 0));
	}

	private void handleQosReport(QosReport data) {
		ensureQosManagerIsRunning();
		this.qosManager.handOffStreamingData(data);
	}

	private void processQosManagerConfig(
			DeployInstanceQosRolesAction deployRolesAction) {

		ensureQosManagerIsRunning();
		this.qosManager.handOffStreamingData(deployRolesAction);
	}

	private synchronized void ensureQosManagerIsRunning() {
		if (this.qosManager == null) {
			this.qosManager = new QosManagerThread(this.jobID,
					this.messagingThread);
			this.qosManager.start();
		}
	}

	private void handleLimitBufferSizeAction(LimitBufferSizeAction action) {

		StreamTaskQosCoordinator qosCoordinator = this.taskQosCoordinators
				.get(action.getVertexID());

		if (qosCoordinator != null) {
			qosCoordinator.handleLimitBufferSizeAction(action);
		} else {
			LOG.error("Cannot find QoS coordinator for vertex with ID "
					+ action.getVertexID());
		}
	}

	private void handleConstructStreamChainAction(
			ConstructStreamChainAction action) {
		
		//FIXME
//		StreamTaskQosCoordinator qosCoordinator = this.taskQosCoordinators
//				.get(action.getVertexID());
//
//		if (qosCoordinator != null) {
//			qosCoordinator.handleLimitBufferSizeAction(action);
//		} else {
//			LOG.error("Cannot find QoS coordinator for vertex with ID "
//					+ action.getVertexID());
//		}
	}

	public void unregisterTask(ExecutionVertexID vertexID,
			Environment environment) {

		// FIXME
	}
}
