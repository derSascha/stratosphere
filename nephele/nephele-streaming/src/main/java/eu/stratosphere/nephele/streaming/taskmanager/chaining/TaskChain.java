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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosStatistic;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosValue;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class TaskChain {

	private static final Logger LOG = Logger.getLogger(TaskChain.class);

	private final AtomicBoolean taskControlFlowsUnderManipulation = new AtomicBoolean(
			false);

	private ArrayList<TaskInfo> tasksInChain = new ArrayList<TaskInfo>();

	private QosStatistic aggregateCpuUtilization = new QosStatistic(
			TaskInfo.CPU_STATISTIC_WINDOW_SIZE);

	public TaskChain(TaskInfo task) {
		this.tasksInChain.add(task);
		task.invalidateCPUUtilizationMeasurements();
	}

	/**
	 * For internal use only.
	 */
	protected TaskChain() {
	}

	private void invalidateCpuUtilMeasurements() {
		for (TaskInfo task : this.tasksInChain) {
			task.invalidateCPUUtilizationMeasurements();
		}
	}

	public void measureCPUUtilizationIfPossible() {
		if (this.taskControlFlowsUnderManipulation.get()) {
			return;
		}

		double aggregateUtilization = 0;
		for (TaskInfo task : this.tasksInChain) {
			task.measureCpuUtilization();
			aggregateUtilization += task.getCPUUtilization();
		}

		this.aggregateCpuUtilization.addValue(new QosValue(
				aggregateUtilization, System.currentTimeMillis()));
	}

	public boolean hasCPUUtilizationMeasurements() {
		return this.aggregateCpuUtilization.hasValues();
	}

	public double getCPUUtilization() {
		return this.aggregateCpuUtilization.getArithmeticMean();
	}

	public void invalidateCPUUtilizationMeasurements() {
		this.aggregateCpuUtilization = new QosStatistic(
				TaskInfo.CPU_STATISTIC_WINDOW_SIZE);
	}

	public int getNumberOfChainedTasks() {
		return this.tasksInChain.size();
	}

	public TaskInfo getTask(int index) {
		return this.tasksInChain.get(index);
	}

	public static Pair<TaskChain, TaskChain> split(TaskChain chain,
			int splitIndex, ExecutorService backgroundWorkers) {

		final TaskChain newLeftChain = new TaskChain();
		final TaskChain newRightChain = new TaskChain();

		chain.invalidateCPUUtilizationMeasurements();
		newLeftChain.tasksInChain = new ArrayList<TaskInfo>(
				chain.tasksInChain.subList(0, splitIndex));
		newRightChain.tasksInChain = new ArrayList<TaskInfo>(
				chain.tasksInChain.subList(splitIndex,
						chain.tasksInChain.size()));

		newLeftChain.taskControlFlowsUnderManipulation.set(true);
		newRightChain.taskControlFlowsUnderManipulation.set(true);

		backgroundWorkers.execute(new Runnable() {
			@Override
			public void run() {
				try {
					ChainingUtil.splitControlFlowsInTasks(newLeftChain,
							newRightChain);
				} catch (Exception e) {
					LOG.error("Error during chain construction.", e);
				} finally {
					newLeftChain.taskControlFlowsUnderManipulation.set(false);
					newRightChain.taskControlFlowsUnderManipulation.set(false);

				}
			}
		});

		return Pair.of(newLeftChain, newRightChain);
	}

	public static TaskChain merge(List<TaskChain> subchains,
			ExecutorService backgroundWorkers) {

		final TaskChain mergedChain = new TaskChain();

		for (TaskChain subchain : subchains) {
			mergedChain.tasksInChain.addAll(subchain.tasksInChain);
		}

		mergedChain.invalidateCpuUtilMeasurements();
		mergedChain.taskControlFlowsUnderManipulation.set(true);

		backgroundWorkers.execute(new Runnable() {
			@Override
			public void run() {
				try {
					ChainingUtil.mergeControlFlowsInTasks(mergedChain);
				} catch (Exception e) {
					LOG.error("Error during chain construction.", e);
				} finally {
					mergedChain.taskControlFlowsUnderManipulation.set(false);
				}
			}
		});

		return mergedChain;
	}
}
