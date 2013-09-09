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
package eu.stratosphere.nephele.streaming.taskmanager.chaining;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.profiling.impl.EnvironmentThreadSet;
import eu.stratosphere.nephele.streaming.message.action.CandidateChainConfig;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class ChainManagerThread extends Thread implements ExecutionListener {

	private final static Logger LOG = Logger
			.getLogger(ChainManagerThread.class);

	private final ConcurrentHashMap<ExecutionVertexID, TaskInfo> taskInfos;

	private final LinkedBlockingQueue<CandidateChainConfig> pendingChainConfigs;

	private ThreadMXBean tmx;

	private boolean started;

	public ChainManagerThread() throws ProfilingException {
		this.taskInfos = new ConcurrentHashMap<ExecutionVertexID, TaskInfo>();
		this.pendingChainConfigs = new LinkedBlockingQueue<CandidateChainConfig>();

		// Initialize MX interface and check if thread contention monitoring is
		// supported
		this.tmx = ManagementFactory.getThreadMXBean();
		if (this.tmx.isThreadContentionMonitoringSupported()) {
			this.tmx.setThreadContentionMonitoringEnabled(true);
		} else {
			throw new ProfilingException(
					"The thread contention monitoring is not supported.");
		}

		this.started = false;
	}

	@Override
	public void run() {
		int counter = 0;
		try {
			while (!interrupted()) {
				this.processPendingChainConfigs();
				this.collectThreadProfilingData();

				if (counter == 0) {
					this.attemptChainConstruction();
				}

				counter = (counter + 1) % 5;
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {

		} finally {
			cleanUp();
		}
	}

	private void attemptChainConstruction() {

	}

	private void collectThreadProfilingData() {
		for (TaskInfo taskInfo : this.taskInfos.values()) {
			taskInfo.measureCpuUtilization(this.tmx);
		}
	}

	private void processPendingChainConfigs() {
		Iterator<CandidateChainConfig> iter = this.pendingChainConfigs
				.iterator();

		while (iter.hasNext()) {
			CandidateChainConfig chainConfig = iter.next();

			boolean chainConfigFullyAssociated = true;

			for (ExecutionVertexID vertexID : chainConfig
					.getChainingCandidates()) {
				if (this.taskInfos.containsKey(vertexID)) {
					TaskInfo taskInfo = this.taskInfos.get(vertexID);
					taskInfo.setChainConfig(chainConfig);
				} else {
					chainConfigFullyAssociated = false;
				}
			}

			if (chainConfigFullyAssociated) {
				iter.remove();
			}
		}
	}

	private void cleanUp() {
		// FIXME clean up data structures here
	}

	public void shutdown() {
		this.interrupt();
	}

	public synchronized void registerMapperTask(RuntimeTask task) {
		this.taskInfos.putIfAbsent(task.getVertexID(), new TaskInfo(task));
		task.registerExecutionListener(this);

		if (!this.started) {
			this.started = true;
			this.start();
		}
	}

	public synchronized void unregisterMapperTask(ExecutionVertexID vertexID) {
		TaskInfo taskInfo = this.taskInfos.get(vertexID);
		if (taskInfo != null) {
			taskInfo.getTask().unregisterExecutionListener(this);
			this.taskInfos.remove(vertexID);
		}

		if (this.taskInfos.isEmpty()) {
			shutdown();
		}
	}

	public void registerCandidateChain(CandidateChainConfig chainConfig) {
		this.pendingChainConfigs.add(chainConfig);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.execution.ExecutionListener#executionStateChanged
	 * (eu.stratosphere.nephele.jobgraph.JobID,
	 * eu.stratosphere.nephele.executiongraph.ExecutionVertexID,
	 * eu.stratosphere.nephele.execution.ExecutionState, java.lang.String)
	 */
	@Override
	public void executionStateChanged(JobID jobID, ExecutionVertexID vertexID,
			ExecutionState newExecutionState, String optionalMessage) {

		TaskInfo taskInfo = this.taskInfos.get(vertexID);

		switch (newExecutionState) {
		case RUNNING:
			taskInfo.setEnvironmentThreadSet(new EnvironmentThreadSet(this.tmx,
					taskInfo.getTask().getRuntimeEnvironment()
							.getExecutingThread(), vertexID));
			break;
		case FINISHING:
		case FINISHED:
		case CANCELING:
		case CANCELED:
		case FAILED:
			taskInfo.setEnvironmentThreadSet(null);
			break;
		default:
			break;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.execution.ExecutionListener#userThreadStarted
	 * (eu.stratosphere.nephele.jobgraph.JobID,
	 * eu.stratosphere.nephele.executiongraph.ExecutionVertexID,
	 * java.lang.Thread)
	 */
	@Override
	public void userThreadStarted(JobID jobID, ExecutionVertexID vertexID,
			Thread userThread) {

		TaskInfo taskInfo = this.taskInfos.get(vertexID);
		if (taskInfo != null) {
			EnvironmentThreadSet threadSet = taskInfo.getEnvironmentThreadSet();
			if (threadSet != null) {
				// threadsafe operation
				threadSet.addUserThread(this.tmx, userThread);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.execution.ExecutionListener#userThreadFinished
	 * (eu.stratosphere.nephele.jobgraph.JobID,
	 * eu.stratosphere.nephele.executiongraph.ExecutionVertexID,
	 * java.lang.Thread)
	 */
	@Override
	public void userThreadFinished(JobID jobID, ExecutionVertexID vertexID,
			Thread userThread) {

		TaskInfo taskInfo = this.taskInfos.get(vertexID);
		if (taskInfo != null) {
			EnvironmentThreadSet threadSet = taskInfo.getEnvironmentThreadSet();
			if (threadSet != null) {
				// threadsafe operation
				threadSet.removeUserThread(userThread);
			}
		}

	}
}
