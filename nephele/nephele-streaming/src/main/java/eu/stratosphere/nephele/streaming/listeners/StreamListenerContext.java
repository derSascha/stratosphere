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

package eu.stratosphere.nephele.streaming.listeners;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.execution.Mapper;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.actions.AbstractAction;
import eu.stratosphere.nephele.streaming.chaining.StreamChain;
import eu.stratosphere.nephele.streaming.chaining.StreamChainCoordinator;
import eu.stratosphere.nephele.streaming.profiling.StreamProfilingReporterThread;
import eu.stratosphere.nephele.streaming.wrappers.StreamingInputGate;
import eu.stratosphere.nephele.streaming.wrappers.StreamingOutputGate;
import eu.stratosphere.nephele.types.Record;

public final class StreamListenerContext {

	public static final String CONTEXT_CONFIGURATION_KEY = "streaming.listener.context";

	public static enum TaskType {
		INPUT, REGULAR, OUTPUT
	};

	private final LinkedBlockingQueue<AbstractAction> pendingActions = new LinkedBlockingQueue<AbstractAction>();

	private final JobID jobID;

	private final ExecutionVertexID vertexID;

	private final StreamProfilingReporterThread profilingReporter;

	private final StreamChainCoordinator chainCoordinator;

	private final TaskType taskType;

	private final int aggregationInterval;

	private final int taggingInterval;

	private StreamListenerContext(final JobID jobID, final ExecutionVertexID vertexID,
			final StreamProfilingReporterThread profilingReporter, final StreamChainCoordinator chainCoordinator,
			final TaskType taskType, final int aggregationInterval, final int taggingInterval) {

		if (jobID == null) {
			throw new IllegalArgumentException("Parameter jobID must not be null");
		}

		if (vertexID == null) {
			throw new IllegalArgumentException("Parameter vertexID must not be null");
		}

		if (profilingReporter == null) {
			throw new IllegalArgumentException("Parameter profilingReporter must not be null");
		}

		if (taskType == null) {
			throw new IllegalArgumentException("Parameter taskType must not be null");
		}

		if (aggregationInterval <= 0) {
			throw new IllegalArgumentException("Parameter aggregationInterval must be greater than zero");
		}

		if (taggingInterval <= 0) {
			throw new IllegalArgumentException("Parameter taggingInterval must be greater than zero");
		}

		this.jobID = jobID;
		this.vertexID = vertexID;
		this.profilingReporter = profilingReporter;
		this.chainCoordinator = chainCoordinator;
		this.taskType = taskType;
		this.aggregationInterval = aggregationInterval;
		this.taggingInterval = taggingInterval;
	}

	public static StreamListenerContext createForInputTask(final JobID jobID, final ExecutionVertexID vertexID,
			final StreamProfilingReporterThread profilingReporter, final StreamChainCoordinator chainCoordinator,
			final int aggregationInterval, final int taggingInterval) {

		return new StreamListenerContext(jobID, vertexID, profilingReporter, chainCoordinator, TaskType.INPUT,
			aggregationInterval, taggingInterval);
	}

	public static StreamListenerContext createForRegularTask(final JobID jobID, final ExecutionVertexID vertexID,
			final StreamProfilingReporterThread profilingReporter, final StreamChainCoordinator chainCoordinator,
			final int aggregationInterval, int taggingInterval) {

		return new StreamListenerContext(jobID, vertexID, profilingReporter, chainCoordinator, TaskType.REGULAR,
			aggregationInterval, taggingInterval);
	}

	public static StreamListenerContext createForOutputTask(final JobID jobID, final ExecutionVertexID vertexID,
			final StreamProfilingReporterThread profilingReporter, final StreamChainCoordinator chainCoordinator,
			final int aggregationInterval, int taggingInterval) {

		return new StreamListenerContext(jobID, vertexID, profilingReporter, chainCoordinator, TaskType.OUTPUT,
			aggregationInterval, taggingInterval);
	}

	boolean isInputVertex() {

		return (this.taskType == TaskType.INPUT);
	}

	boolean isOutputVertex() {

		return (this.taskType == TaskType.OUTPUT);
	}

	boolean isRegularVertex() {

		return (this.taskType == TaskType.REGULAR);
	}

	TaskType getTaskType() {
		return this.taskType;
	}

	public JobID getJobID() {

		return this.jobID;
	}

	public ExecutionVertexID getVertexID() {

		return this.vertexID;
	}

	public int getTaggingInterval() {

		return this.taggingInterval;
	}

	public int getAggregationInterval() {

		return this.aggregationInterval;
	}

	public StreamProfilingReporterThread getProfilingReporter() {
		return profilingReporter;
	}

	public void queuePendingAction(final AbstractAction action) {
		this.pendingActions.add(action);
	}

	LinkedBlockingQueue<AbstractAction> getPendingActionsQueue() {
		return this.pendingActions;
	}

	<I extends Record, O extends Record> void registerMapper(final Mapper<I, O> mapper,
			final StreamingInputGate<I> inputGate, final StreamingOutputGate<O> outputGate) {

		this.chainCoordinator.registerMapper(this.vertexID, mapper, inputGate, outputGate);
	}

	StreamChain constructStreamChain(final List<ExecutionVertexID> vertexIDs) {

		return this.chainCoordinator.constructStreamChain(vertexIDs);
	}
}
