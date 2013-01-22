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

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.executiongraph.JobStatusListener;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.AbstractJobInputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.plugins.JobManagerPlugin;
import eu.stratosphere.nephele.plugins.PluginID;
import eu.stratosphere.nephele.profiling.ProfilingListener;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamInputTaskWrapper;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamOutputTaskWrapper;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskWrapper;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.WrapperUtils;
import eu.stratosphere.nephele.template.AbstractInvokable;

public class StreamJobManagerPlugin implements JobManagerPlugin,
		JobStatusListener {

	private static final Log LOG = LogFactory
			.getLog(StreamJobManagerPlugin.class);

	private final PluginID pluginID;

	private final Configuration pluginConfiguration;

	private ConcurrentHashMap<JobID, QosSetupManager> streamProfilingManagers = new ConcurrentHashMap<JobID, QosSetupManager>();

	public StreamJobManagerPlugin(final PluginID pluginID,
			final Configuration pluginConfiguration) {
		this.pluginID = pluginID;
		this.pluginConfiguration = pluginConfiguration;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobGraph rewriteJobGraph(final JobGraph jobGraph) {
		this.rewriteInputVertices(jobGraph);
		this.rewriteTaskVertices(jobGraph);
		this.rewriteOutputVertices(jobGraph);

		return jobGraph;
	}

	private void rewriteOutputVertices(final JobGraph jobGraph) {
		final Iterator<AbstractJobOutputVertex> outputIt = jobGraph
				.getOutputVertices();
		while (outputIt.hasNext()) {

			final AbstractJobOutputVertex abstractOutputVertex = outputIt
					.next();
			final Class<? extends AbstractInvokable> originalClass = abstractOutputVertex
					.getInvokableClass();

			if (abstractOutputVertex instanceof JobOutputVertex) {
				final JobOutputVertex outputVertex = (JobOutputVertex) abstractOutputVertex;
				outputVertex.setOutputClass(StreamOutputTaskWrapper.class);
			} else {
				LOG.warn("Cannot wrap output task of type " + originalClass
						+ ", skipping...");
				continue;
			}

			abstractOutputVertex.getConfiguration().setString(
					WrapperUtils.WRAPPED_CLASS_KEY, originalClass.getName());
		}
	}

	private void rewriteTaskVertices(final JobGraph jobGraph) {
		final Iterator<JobTaskVertex> taskIt = jobGraph.getTaskVertices();
		while (taskIt.hasNext()) {

			final JobTaskVertex taskVertex = taskIt.next();
			final Class<? extends AbstractInvokable> originalClass = taskVertex
					.getInvokableClass();
			taskVertex.setTaskClass(StreamTaskWrapper.class);
			taskVertex.getConfiguration().setString(
					WrapperUtils.WRAPPED_CLASS_KEY, originalClass.getName());
		}
	}

	private void rewriteInputVertices(final JobGraph jobGraph) {
		final Iterator<AbstractJobInputVertex> inputIt = jobGraph
				.getInputVertices();
		while (inputIt.hasNext()) {

			final AbstractJobInputVertex abstractInputVertex = inputIt.next();
			final Class<? extends AbstractInvokable> originalClass = abstractInputVertex
					.getInvokableClass();

			if (abstractInputVertex instanceof JobInputVertex) {
				final JobInputVertex inputVertex = (JobInputVertex) abstractInputVertex;
				inputVertex.setInputClass(StreamInputTaskWrapper.class);
			} else {
				LOG.warn("Cannot wrap input task of type " + originalClass
						+ ", skipping...");
				continue;
			}

			abstractInputVertex.getConfiguration().setString(
					WrapperUtils.WRAPPED_CLASS_KEY, originalClass.getName());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionGraph rewriteExecutionGraph(
			final ExecutionGraph executionGraph) {
		JobID jobId = executionGraph.getJobID();
		QosSetupManager profilingManager = new QosSetupManager(
				executionGraph);
		this.streamProfilingManagers.put(jobId, profilingManager);

		return executionGraph;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {
		for (QosSetupManager streamProfilingManager : this.streamProfilingManagers
				.values()) {
			streamProfilingManager.shutdown();
		}
		this.streamProfilingManagers.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void sendData(final IOReadableWritable data) throws IOException {
		LOG.error("Job manger streaming plugin received unexpected data of type "
				+ data);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOReadableWritable requestData(final IOReadableWritable data)
			throws IOException {
		LOG.error("Job manger streaming plugin received unexpected data request of type "
				+ data);
		return null;
	}

	@Override
	public void jobStatusHasChanged(ExecutionGraph executionGraph,
			InternalJobStatus newJobStatus, String optionalMessage) {

		if (newJobStatus == InternalJobStatus.FAILED
				|| newJobStatus == InternalJobStatus.CANCELED
				|| newJobStatus == InternalJobStatus.FINISHED) {

			QosSetupManager streamProfilingManager = this.streamProfilingManagers
					.remove(executionGraph.getJobID());

			if (streamProfilingManager != null) {
				streamProfilingManager.shutdown();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean requiresProfiling() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ProfilingListener getProfilingListener(final JobID jobID) {
		return null;
	}

	public PluginID getPluginID() {
		return this.pluginID;
	}

	public Configuration getPluginConfiguration() {
		return this.pluginConfiguration;
	}
}
