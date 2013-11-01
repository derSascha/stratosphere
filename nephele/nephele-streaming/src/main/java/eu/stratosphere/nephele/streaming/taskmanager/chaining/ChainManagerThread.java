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
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.streaming.message.action.CandidateChainConfig;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class ChainManagerThread extends Thread {

	private final static Logger LOG = Logger
			.getLogger(ChainManagerThread.class);

	private final ExecutorService backgroundChainingWorkers;

	private final ConcurrentHashMap<ExecutionVertexID, TaskInfo> activeMapperTasks;

	private final CopyOnWriteArraySet<CandidateChainConfig> pendingCandidateChainConfigs;

	private final ArrayList<TaskChainer> taskChainers;

	private ThreadMXBean tmx;

	private boolean started;

	public ChainManagerThread() throws ProfilingException {
		this.backgroundChainingWorkers = Executors.newCachedThreadPool();
		this.activeMapperTasks = new ConcurrentHashMap<ExecutionVertexID, TaskInfo>();
		this.pendingCandidateChainConfigs = new CopyOnWriteArraySet<CandidateChainConfig>();
		this.taskChainers = new ArrayList<TaskChainer>();

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
				this.processPendingCandidateChainConfigs();
				this.collectThreadProfilingData();

				if (counter == 5) {
					this.attemptDynamicChaining();
					counter = 0;
				}

				counter++;
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {

		} finally {
			cleanUp();
		}
	}

	private void attemptDynamicChaining() {
		for (TaskChainer taskChainer : this.taskChainers) {
			taskChainer.attemptDynamicChaining();
		}
	}

	private void processPendingCandidateChainConfigs() {
		for (CandidateChainConfig candidateChainConfig : this.pendingCandidateChainConfigs) {
			boolean success = tryToCreateTaskChainer(candidateChainConfig);
			if (success) {
				this.pendingCandidateChainConfigs.remove(candidateChainConfig);
			}
		}
	}

	private boolean tryToCreateTaskChainer(
			CandidateChainConfig candidateChainConfig) {

		ArrayList<TaskInfo> taskInfos = new ArrayList<TaskInfo>();
		for (ExecutionVertexID vertexID : candidateChainConfig
				.getChainingCandidates()) {

			TaskInfo taskInfo = this.activeMapperTasks.get(vertexID);
			if (taskInfo == null) {
				return false;
			}

			taskInfos.add(taskInfo);
		}

		this.taskChainers.add(new TaskChainer(taskInfos,
				this.backgroundChainingWorkers));

		return true;
	}

	private void collectThreadProfilingData() {
		for (TaskChainer taskChainer : this.taskChainers) {
			taskChainer.measureCPUUtilizations();
		}
	}

	private void cleanUp() {
		// FIXME clean up data structures here
	}

	public void shutdown() {
		this.interrupt();
	}

	public synchronized void registerMapperTask(RuntimeTask task) {
		TaskInfo taskInfo = new TaskInfo(task, this.tmx);
		this.activeMapperTasks.put(task.getVertexID(), taskInfo);

		if (!this.started) {
			this.started = true;
			this.start();
		}
	}

	public synchronized void unregisterMapperTask(ExecutionVertexID vertexID) {
		TaskInfo taskInfo = this.activeMapperTasks.remove(vertexID);
		if (taskInfo != null) {
			taskInfo.cleanUp();
		}

		if (this.activeMapperTasks.isEmpty()) {
			shutdown();
		}
	}

	public void registerCandidateChain(CandidateChainConfig chainConfig) {
		this.pendingCandidateChainConfigs.add(chainConfig);
	}
}
