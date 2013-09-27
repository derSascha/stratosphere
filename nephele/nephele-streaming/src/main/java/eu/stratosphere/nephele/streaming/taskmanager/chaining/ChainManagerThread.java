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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;

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

	private final ConcurrentHashMap<ExecutionVertexID, TaskInfo> activeMapperTasks;

	private final CopyOnWriteArraySet<CandidateChainConfig> candidateChains;
	
	

	private ThreadMXBean tmx;

	private boolean started;

	public ChainManagerThread() throws ProfilingException {
		this.activeMapperTasks = new ConcurrentHashMap<ExecutionVertexID, TaskInfo>();
		this.candidateChains = new CopyOnWriteArraySet<CandidateChainConfig>();

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
		for (CandidateChainConfig candidateChain : this.candidateChains) {
			attemptChainConstruction(candidateChain);
		}
	}

	private void attemptChainConstruction(CandidateChainConfig candidateChain) {
		ChainInfo longestPossibleChain = findLongestPossibleChain(candidateChain);
		// FIXME activate chain
	}

	private ChainInfo findLongestPossibleChain(
			CandidateChainConfig candidateChain) {

		ArrayList<TaskInfo> tasks = getTasksReadyForChaining(candidateChain);

		int longestChainStart = -1;
		int longestChainLength = 0;
		double longestChainCPUUsage = 0;

		for (int chainStart = 0; chainStart < tasks.size() - 1; chainStart++) {

			double chainCPUUsage = 0;

			int chainEnd;

			for (chainEnd = chainStart; chainEnd < tasks.size(); chainEnd++) {
				TaskInfo taskInfo = tasks.get(chainEnd);

				if (taskInfo == null) {
					chainEnd--;
					break;
				}

				double taskCPUUtilization = taskInfo.getCPUUtilization();
				chainCPUUsage += taskCPUUtilization;

				if (chainCPUUsage > 90.0) {
					chainCPUUsage -= taskCPUUtilization;
					chainEnd--;
					break;
				}
			}

			int chainLength = chainEnd + 1 - chainStart;

			if (chainLength > 1 && chainCPUUsage > longestChainCPUUsage) {
				longestChainStart = chainStart;
				longestChainLength = chainLength;
				longestChainCPUUsage = chainCPUUsage;
			}
		}

		if (longestChainLength > 0) {
			return createChain(candidateChain, tasks, longestChainStart,
					longestChainLength);
		}
		return null;
	}

	private ArrayList<TaskInfo> getTasksReadyForChaining(
			CandidateChainConfig candidateChain) {
		ArrayList<TaskInfo> tasks = new ArrayList<TaskInfo>();

		for (ExecutionVertexID vertexID : candidateChain
				.getChainingCandidates()) {

			TaskInfo taskInfo = this.activeMapperTasks.get(vertexID);
			if (taskInfo != null && taskInfo.hasCPUUtilizationMeasurements()
					&& !taskInfo.isChained()) {
				tasks.add(taskInfo);
			} else {
				tasks.add(null);
			}
		}
		return tasks;
	}

	private ChainInfo createChain(CandidateChainConfig candidateChain,
			ArrayList<TaskInfo> candidateChainMembers, int chainStart,
			int chainLength) {

		TaskInfo nextToChain = candidateChainMembers.get(chainStart);

		ChainInfo chain = new ChainInfo(candidateChain, nextToChain, chainStart);

		StringBuilder logMessage = new StringBuilder("Created chain with tasks: ");
		logMessage.append(nextToChain.getTask().getEnvironment().getTaskName());

		for (int i = chainStart + 1; i < chainStart + chainLength; i++) {
			nextToChain = candidateChainMembers.get(i);
			logMessage.append(", ");
			logMessage.append(nextToChain.getTask().getEnvironment().getTaskName());
			
			chain.appendToChain(candidateChainMembers.get(i));
		}
		
		LOG.info(logMessage.toString());

		return chain;
	}

	private void collectThreadProfilingData() {
		for (TaskInfo taskInfo : this.activeMapperTasks.values()) {
			taskInfo.measureCpuUtilization(this.tmx);
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
		this.candidateChains.add(chainConfig);
	}
}
