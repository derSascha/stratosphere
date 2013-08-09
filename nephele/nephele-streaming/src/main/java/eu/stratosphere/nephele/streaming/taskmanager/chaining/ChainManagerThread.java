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

import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.action.CandidateChainConfig;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class ChainManagerThread extends Thread implements ExecutionListener {

	private LinkedBlockingQueue<RuntimeTask> newStreamTasks;

	public ChainManagerThread() {

	}

	@Override
	public void run() {
		try {
			while (!interrupted()) {
				this.wait(); // FIXME
			}
		} catch (InterruptedException e) {

		} finally {
			cleanUp();
		}
	}

	private void cleanUp() {
		// FIXME clean up data structures here
	}

	public void shutdown() {
		this.interrupt();
	}

	public void registerMapperTask(RuntimeTask task) {
		// FIXME

	}

	public void registerCandidateChain(CandidateChainConfig chainConfig) {
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.execution.ExecutionListener#executionStateChanged(eu.stratosphere.nephele.jobgraph.JobID, eu.stratosphere.nephele.executiongraph.ExecutionVertexID, eu.stratosphere.nephele.execution.ExecutionState, java.lang.String)
	 */
	@Override
	public void executionStateChanged(JobID jobID, ExecutionVertexID vertexID,
			ExecutionState newExecutionState, String optionalMessage) {
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.execution.ExecutionListener#userThreadStarted(eu.stratosphere.nephele.jobgraph.JobID, eu.stratosphere.nephele.executiongraph.ExecutionVertexID, java.lang.Thread)
	 */
	@Override
	public void userThreadStarted(JobID jobID, ExecutionVertexID vertexID,
			Thread userThread) {
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.execution.ExecutionListener#userThreadFinished(eu.stratosphere.nephele.jobgraph.JobID, eu.stratosphere.nephele.executiongraph.ExecutionVertexID, java.lang.Thread)
	 */
	@Override
	public void userThreadFinished(JobID jobID, ExecutionVertexID vertexID,
			Thread userThread) {
	}
}
