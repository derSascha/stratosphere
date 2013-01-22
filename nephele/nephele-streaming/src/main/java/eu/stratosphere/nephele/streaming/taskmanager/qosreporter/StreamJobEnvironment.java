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
import eu.stratosphere.nephele.streaming.message.action.ActAsQosManagerAction;
import eu.stratosphere.nephele.streaming.message.action.ActAsQosReporterAction;
import eu.stratosphere.nephele.streaming.message.action.ConstructStreamChainAction;
import eu.stratosphere.nephele.streaming.message.action.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.message.profiling.StreamProfilingReport;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.StreamTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosManagerThread;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.StreamChainCoordinator;

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

	private QosReporterThread qosReporter;

	private QosManagerThread qosManager;

	private StreamChainCoordinator chainCoordinator;

	private HashMap<ExecutionVertexID, StreamTaskQosCoordinator> taskQosCoordinators;

	private StreamMessagingThread messagingThread;

	public StreamJobEnvironment(JobID jobID,
			StreamMessagingThread messagingThread) {

		this.jobID = jobID;
		this.messagingThread = messagingThread;

		QosReporterConfiguration reporterConfig = new QosReporterConfiguration();
		reporterConfig.setAggregationInterval(StreamTaskManagerPlugin
				.getDefaultAggregationInterval());
		reporterConfig.setTaggingInterval(StreamTaskManagerPlugin
				.getDefaultTaggingInterval());
		this.qosReporter = new QosReporterThread(jobID, messagingThread,
				reporterConfig);

		this.chainCoordinator = new StreamChainCoordinator();
		this.taskQosCoordinators = new HashMap<ExecutionVertexID, StreamTaskQosCoordinator>();
	}

	/**
	 * Returns the jobID.
	 * 
	 * @return the jobID
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Returns the qosReporter.
	 * 
	 * @return the qosReporter
	 */
	public QosReporterThread getQosReporter() {
		return this.qosReporter;
	}

	/**
	 * Returns the qosManager.
	 * 
	 * @return the qosManager
	 */
	public QosManagerThread getQosManager() {
		return this.qosManager;
	}

	/**
	 * Returns the chainCoordinator.
	 * 
	 * @return the chainCoordinator
	 */
	public StreamChainCoordinator getChainCoordinator() {
		return this.chainCoordinator;
	}

	public void registerTask(ExecutionVertexID vertexID,
			StreamTaskEnvironment taskEnvironment) {
		
		updateReportingConfiguration(taskEnvironment);

		synchronized (this.taskQosCoordinators) {
			if (this.taskQosCoordinators.containsKey(vertexID)) {
				throw new RuntimeException(String.format(
						"Task %s is already registered",
						taskEnvironment.getTaskName()));
			}
			
			this.taskQosCoordinators.put(vertexID,
					new StreamTaskQosCoordinator(vertexID, taskEnvironment,
							this.qosReporter, this.chainCoordinator));
		}
	}

	/**
	 * @param taskEnvironment
	 */
	private void updateReportingConfiguration(Environment taskEnvironment) {
		long aggregationInterval = taskEnvironment
				.getJobConfiguration()
				.getLong(StreamTaskManagerPlugin.AGGREGATION_INTERVAL_KEY,
						StreamTaskManagerPlugin.getDefaultAggregationInterval());
		int taggingInterval = taskEnvironment.getJobConfiguration().getInteger(
				StreamTaskManagerPlugin.TAGGING_INTERVAL_KEY,
				StreamTaskManagerPlugin.getDefaultTaggingInterval());

		this.qosReporter.getConfiguration().setAggregationInterval(
				aggregationInterval);
		this.qosReporter.getConfiguration().setTaggingInterval(taggingInterval);
	}

	public StreamTaskQosCoordinator getTaskQosCoordinator(
			ExecutionVertexID vertexID) {

		return this.taskQosCoordinators.get(vertexID);
	}

	public void shutdown() {
		if (this.qosManager != null) {
			this.qosManager.shutdown();
		}
		this.qosManager = null;

		this.qosReporter.shutdown();
		this.qosReporter = null;
	}

	/**
	 * @param streamMsg
	 */
	public void handleStreamMessage(AbstractStreamMessage streamMsg) {
		if (streamMsg instanceof StreamProfilingReport) {
			this.handleStreamProfilingReport((StreamProfilingReport) streamMsg);
		} else if (streamMsg instanceof LimitBufferSizeAction) {
			this.handleLimitBufferSizeAction((LimitBufferSizeAction) streamMsg);
		} else if (streamMsg instanceof ConstructStreamChainAction) {
			this.handleConstructStreamChainAction((ConstructStreamChainAction) streamMsg);
		} else if (streamMsg instanceof ActAsQosManagerAction) {
			this.handleActAsQosManagerAction((ActAsQosManagerAction) streamMsg);
		} else if (streamMsg instanceof ActAsQosReporterAction) {
			this.handleActAsQosReporterAction((ActAsQosReporterAction) streamMsg);
		} else {
			LOG.error("Received message is of unknown type "
					+ streamMsg.getClass());
		}
	}

	private void handleStreamProfilingReport(StreamProfilingReport data) {
		if (this.qosManager != null) {
			this.qosManager.handOffStreamingData(data);
		} else {
			LOG.error("Received StreamProfilingReport but could not find QoS manager for job "
					+ data.getJobID());
		}
	}

	private void handleActAsQosReporterAction(
			ActAsQosReporterAction reporterAction) {
		this.qosReporter.registerQosReporter(reporterAction);
	}

	private void handleActAsQosManagerAction(ActAsQosManagerAction action) {

		if (this.qosManager != null) {
			LOG.error("This task manager is already QoS manager for the given job "
					+ this.jobID.toString());
			return;
		}

		this.qosManager = new QosManagerThread(this.jobID,
				this.messagingThread, action.getProfilingSequence());
		this.qosManager.start();
	}

	private void handleLimitBufferSizeAction(LimitBufferSizeAction action) {
		
		StreamTaskQosCoordinator qosCoordinator = this.taskQosCoordinators.get(action.getVertexID());
		
		if(qosCoordinator != null) {
			qosCoordinator.queueAction(action);
		} else {
			LOG.error("Cannot find QoS coordinator for vertex with ID "+ action.getVertexID());			
		}
	}

	private void handleConstructStreamChainAction(
			ConstructStreamChainAction action) {
		
		StreamTaskQosCoordinator qosCoordinator = this.taskQosCoordinators.get(action.getVertexID());
		
		if(qosCoordinator != null) {
			qosCoordinator.queueAction(action);
		} else {
			LOG.error("Cannot find QoS coordinator for vertex with ID "+ action.getVertexID());			
		}
	}

	/**
	 * @param vertexID
	 * @param environment
	 */
	public void unregisterTask(ExecutionVertexID vertexID,
			Environment environment) {

		// FIXME
	}

}
