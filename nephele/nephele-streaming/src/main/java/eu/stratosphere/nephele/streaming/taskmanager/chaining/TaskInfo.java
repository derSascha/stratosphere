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

import java.lang.management.ThreadMXBean;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.profiling.impl.EnvironmentThreadSet;
import eu.stratosphere.nephele.profiling.impl.types.InternalExecutionVertexThreadProfilingData;
import eu.stratosphere.nephele.streaming.message.action.CandidateChainConfig;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosStatistic;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosValue;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class TaskInfo {

	private final RuntimeTask task;

	private volatile CandidateChainConfig chainConfig;

	private volatile EnvironmentThreadSet environmentThreadSet;

	private QosStatistic cpuUtilization;

	public TaskInfo(RuntimeTask task) {
		this.task = task;
		this.cpuUtilization = new QosStatistic(5);
	}

	/**
	 * 
	 * @return the runtimetask
	 */
	public RuntimeTask getTask() {
		return this.task;
	}

	public ExecutionVertexID getVertexID() {
		return this.task.getVertexID();
	}

	/**
	 * 
	 * @return the associated chain configuration or null if none was set
	 */
	public CandidateChainConfig getChainConfig() {
		return this.chainConfig;
	}

	/**
	 * Sets the chainConfig to the specified value.
	 * 
	 * @param chainConfig
	 *            the chainConfig to set
	 */
	public void setChainConfig(CandidateChainConfig chainConfig) {
		this.chainConfig = chainConfig;
	}

	/**
	 * Returns the environmentThreadSet.
	 * 
	 * @return the environmentThreadSet
	 */
	public EnvironmentThreadSet getEnvironmentThreadSet() {
		return this.environmentThreadSet;
	}

	/**
	 * Sets the environmentThreadSet to the specified value.
	 * 
	 * @param environmentThreadSet
	 *            the environmentThreadSet to set
	 */
	public void setEnvironmentThreadSet(
			EnvironmentThreadSet environmentThreadSet) {

		this.environmentThreadSet = environmentThreadSet;
	}

	public ExecutionState getExecutionState() {
		return this.task.getExecutionState();
	}

	public void measureCpuUtilization(ThreadMXBean tmx) {

		if (this.environmentThreadSet != null) {

			long now = System.currentTimeMillis();
			InternalExecutionVertexThreadProfilingData profilingData = this.environmentThreadSet
					.captureCPUUtilization(this.task.getJobID(), tmx, now);

			/**
			 * cpuUtilization measures in percent, how much of one CPU core's
			 * available time the the vertex's main thread AND its associated
			 * user threads have consumed. Example values:
			 * 
			 * cpuUtilization==50 => it uses half a core's worth of CPU time
			 * 
			 * cpuUtilization==200 => it uses two core's worth of CPU time (this
			 * can happen if it spawns several user threads)
			 */
			double cpuUtilization = (profilingData.getUserTime()
					+ profilingData.getSystemTime() + profilingData
						.getBlockedTime())
					* (this.environmentThreadSet.getNumberOfUserThreads() + 1);

			this.cpuUtilization.addValue(new QosValue(cpuUtilization, now));
			
			System.out.printf("Measured %.01f % CPU utilization for vertex %s\n", cpuUtilization);
		}
	}
}
