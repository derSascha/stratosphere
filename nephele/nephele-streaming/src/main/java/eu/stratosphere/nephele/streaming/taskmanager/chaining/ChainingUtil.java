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

/**
 * @author bjoern
 * 
 */
public class ChainingUtil {

	/**
	 * @param flowsToChain
	 *            The sub-dataflows to chain.
	 */
	public static void mergeControlFlowsInTasks(TaskChain dataflow) {
		// FIXME implement
		// StringBuilder logMessage = new StringBuilder(
		// "Created chain with tasks:");
		//
		// for (int i = chainStart; i < chainStart + chainLength; i++) {
		// TaskInfo nextToChain = candidateChainMembers.get(i);
		// logMessage.append(" ");
		// logMessage.append(nextToChain.getTask().getEnvironment()
		// .getTaskName());
		// logMessage.append(nextToChain.getTask().getEnvironment()
		// .getIndexInSubtaskGroup());
		//
		// chain.appendToChain(candidateChainMembers.get(i), i);
		// }
		//
		// LOG.info(logMessage.toString());

	}

	/**
	 * @param newLeftChain
	 * @param runnable
	 */
	public static void splitControlFlowsInTasks(TaskChain leftChain,
			TaskChain rightChain) {
	}
}
