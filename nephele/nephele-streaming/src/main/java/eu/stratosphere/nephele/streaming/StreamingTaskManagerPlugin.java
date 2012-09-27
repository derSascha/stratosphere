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

package eu.stratosphere.nephele.streaming;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.plugins.PluginCommunication;
import eu.stratosphere.nephele.plugins.TaskManagerPlugin;
import eu.stratosphere.nephele.streaming.actions.ActAsProfilingMasterAction;
import eu.stratosphere.nephele.streaming.actions.ConstructStreamChainAction;
import eu.stratosphere.nephele.streaming.actions.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.chaining.StreamChainCoordinator;
import eu.stratosphere.nephele.streaming.listeners.StreamListenerContext;
import eu.stratosphere.nephele.streaming.profiling.JobProfilingDataReporter;
import eu.stratosphere.nephele.streaming.profiling.JobStreamProfilingMasterThread;
import eu.stratosphere.nephele.streaming.types.StreamProfilingReport;
import eu.stratosphere.nephele.streaming.types.TaskProfilingInfo;

public class StreamingTaskManagerPlugin implements TaskManagerPlugin {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory.getLog(StreamingTaskManagerPlugin.class);

	/**
	 * Provides access to the configuration entry which defines the interval in which records shall be tagged.
	 */
	private static final String TAGGING_INTERVAL_KEY = "streaming.profilingreporter.tagginginterval";

	/**
	 * The default tagging interval.
	 */
	private static final int DEFAULT_TAGGING_INTERVAL = 10;

	/**
	 * Provides access to the configuration entry which defines the interval in which received tags shall be aggregated
	 * and sent to the job manager plugin component.
	 */
	private static final String AGGREGATION_INTERVAL_KEY = "streaming.aggregation.interval";

	/**
	 * The default aggregation interval.
	 */
	private static final int DEFAULT_AGGREGATION_INTERVAL = 10;

	/**
	 * Stores the instance of the streaming task manager plugin.
	 */
	private static volatile StreamingTaskManagerPlugin INSTANCE = null;

	/**
	 * Map storing the listener context objects by stringified {@link ExecutionVertexID} for the individual stream
	 * listeners.
	 */
	private final ConcurrentMap<String, StreamListenerContext> listenerContexts = new ConcurrentHashMap<String, StreamListenerContext>();

	private final ConcurrentMap<ExecutionVertexID, TaskProfilingInfo> taskProfilingInfos = new ConcurrentHashMap<ExecutionVertexID, TaskProfilingInfo>();

	/**
	 * Map storing the stream profiling reporter object for each job running on the task manager.
	 */
	private final ConcurrentMap<JobID, JobProfilingDataReporter> profilingReporters = new ConcurrentHashMap<JobID, JobProfilingDataReporter>();

	private final ConcurrentMap<JobID, JobStreamProfilingMasterThread> profilingMasters = new ConcurrentHashMap<JobID, JobStreamProfilingMasterThread>();

	/**
	 * The tagging interval as specified in the plugin configuration.
	 */
	private final int taggingInterval;

	/**
	 * The aggregation interval as specified in the plugin configuration.
	 */
	private final int aggregationInterval;

	/**
	 * A special thread to asynchronously send data to other task managers without suffering from the RPC latency.
	 */
	private final StreamingCommunicationThread communicationThread;

	private final StreamChainCoordinator chainCoordinator;

	private static volatile Configuration PLUGIN_CONFIGURATION;

	StreamingTaskManagerPlugin(final Configuration pluginConfiguration, final PluginCommunication jobManagerComponent) {
		this.taggingInterval = pluginConfiguration.getInteger(TAGGING_INTERVAL_KEY, DEFAULT_TAGGING_INTERVAL);
		this.aggregationInterval = pluginConfiguration.getInteger(AGGREGATION_INTERVAL_KEY,
			DEFAULT_AGGREGATION_INTERVAL);

		this.communicationThread = new StreamingCommunicationThread();
		this.communicationThread.start();

		this.chainCoordinator = new StreamChainCoordinator();

		LOG.info(String.format("Configured tagging interval is every %d records / Aggregation interval is %d millis ",
			this.taggingInterval, this.aggregationInterval));

		INSTANCE = this;
		PLUGIN_CONFIGURATION = pluginConfiguration;
	}

	public static StreamListenerContext getStreamingListenerContext(final String listenerKey) {

		if (INSTANCE == null) {
			throw new IllegalStateException("StreamingTaskManagerPlugin has not been initialized");
		}

		return INSTANCE.listenerContexts.get(listenerKey);
	}

	public static Configuration getPluginConfiguration() {
		return PLUGIN_CONFIGURATION;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {
		this.communicationThread.stopCommunicationThread();

		for (JobStreamProfilingMasterThread profilingMaster : this.profilingMasters.values()) {
			profilingMaster.shutdown();
		}
		this.profilingMasters.clear();
		this.taskProfilingInfos.clear();
		for (JobProfilingDataReporter reporter : this.profilingReporters.values()) {
			reporter.shutdown();
		}
		this.profilingReporters.clear();

		INSTANCE = null;
		PLUGIN_CONFIGURATION = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerTask(final ExecutionVertexID id, final Configuration jobConfiguration,
			final Environment environment, final IOReadableWritable pluginData) {

		if (pluginData == null) {
			return;
		}

		final TaskProfilingInfo taskProfilingInfo = (TaskProfilingInfo) pluginData;
		taskProfilingInfos.put(taskProfilingInfo.getVertexID(), taskProfilingInfo);

		// Check if user has provided a job-specific aggregation interval
		final int aggregationInterval = jobConfiguration.getInteger(AGGREGATION_INTERVAL_KEY,
			this.aggregationInterval);

		final int taggingInterval = jobConfiguration.getInteger(TAGGING_INTERVAL_KEY, this.taggingInterval);

		final String idAsString = id.toString();

		environment.getTaskConfiguration().setString(StreamListenerContext.CONTEXT_CONFIGURATION_KEY, idAsString);

		final JobID jobID = environment.getJobID();
		JobProfilingDataReporter profilingReporter = getOrCreateStreamProfilingReporter(jobID);
		profilingReporter.registerProfilingInfo(taskProfilingInfo);

		StreamListenerContext listenerContext = null;
		if (environment.getNumberOfInputGates() == 0) {
			listenerContext = StreamListenerContext.createForInputTask(jobID, id, profilingReporter,
				this.chainCoordinator, aggregationInterval, taggingInterval);
		} else if (environment.getNumberOfOutputGates() == 0) {
			listenerContext = StreamListenerContext.createForOutputTask(jobID, id, profilingReporter,
				this.chainCoordinator, aggregationInterval, taggingInterval);
		} else {
			listenerContext = StreamListenerContext.createForRegularTask(jobID, id, profilingReporter,
				this.chainCoordinator, aggregationInterval, taggingInterval);
		}

		this.listenerContexts.putIfAbsent(idAsString, listenerContext);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterTask(final ExecutionVertexID id, final Environment environment) {

		this.listenerContexts.remove(id.toString());

		TaskProfilingInfo taskProfilingInfo = taskProfilingInfos.remove(id);

		if (taskProfilingInfo != null) {
			final JobID jobID = environment.getJobID();
			JobProfilingDataReporter profilingReporter = getOrCreateStreamProfilingReporter(jobID);
			boolean isShutDown = profilingReporter.unregisterTaskProfilingInfo(taskProfilingInfo);
			if (isShutDown) {
				synchronized (profilingReporters) {
					profilingReporters.remove(jobID);
				}
			}
		}
	}

	private JobProfilingDataReporter getOrCreateStreamProfilingReporter(JobID jobID) {
		// synchronized to prevent race conditions between containsKey() and put()
		synchronized (profilingReporters) {
			if (!profilingReporters.containsKey(jobID)) {
				profilingReporters.putIfAbsent(jobID, new JobProfilingDataReporter(jobID, communicationThread,
					aggregationInterval));
			}
			return profilingReporters.get(jobID);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void sendData(final IOReadableWritable data) throws IOException {
		if (data instanceof StreamProfilingReport) {
			handleStreamProfilingReport((StreamProfilingReport) data);
		} else if (data instanceof ConstructStreamChainAction) {
			handleConstructStreamChainAction((ConstructStreamChainAction) data);
		} else if (data instanceof LimitBufferSizeAction) {
			handleLimitBufferSizeAction((LimitBufferSizeAction) data);
		} else if (data instanceof ActAsProfilingMasterAction) {
			handleActAsProfilingMasterAction((ActAsProfilingMasterAction) data);
		} else {
			LOG.error("Received data is of unknown type " + data.getClass());
		}
	}

	private void handleStreamProfilingReport(StreamProfilingReport data) {
		JobStreamProfilingMasterThread profilingMaster = profilingMasters.get(data.getJobID());
		if (profilingMaster == null) {
			LOG.error("Received StreamProfilingReport but could not find profiling master for job " + data.getJobID());
			return;
		}
		profilingMaster.handOffStreamingData(data);
	}

	private void handleActAsProfilingMasterAction(ActAsProfilingMasterAction action) {
		// synchronized to prevent race conditions between containsKey() and put()
		synchronized (profilingMasters) {
			JobID jobID = action.getJobID();
			if (profilingMasters.containsKey(jobID)) {
				LOG.error("This task manager is already profiling master for the given job " + jobID.toString());
				return;
			}

			JobStreamProfilingMasterThread profilingMaster = new JobStreamProfilingMasterThread(jobID, communicationThread,
				action.getProfilingSequence());
			profilingMasters.put(jobID, profilingMaster);
			profilingMaster.start();
		}
	}

	private void handleLimitBufferSizeAction(LimitBufferSizeAction action) {
		final StreamListenerContext listenerContext = this.listenerContexts.get(action.getVertexID().toString());
		if (listenerContext == null) {
			LOG.error("Cannot find listener context for vertex with ID " + action.getVertexID());
			return;
		}

		// Queue the action and return
		listenerContext.queuePendingAction(action);
	}

	private void handleConstructStreamChainAction(ConstructStreamChainAction action) {
		final StreamListenerContext listenerContext = this.listenerContexts.get(action.getVertexID().toString());
		if (listenerContext == null) {
			LOG.error("Cannot find listener context for vertex with ID " + action.getVertexID());
			return;
		}

		// Queue the action and return
		listenerContext.queuePendingAction(action);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOReadableWritable requestData(final IOReadableWritable data) throws IOException {

		// TODO Implement me

		return null;
	}

}
