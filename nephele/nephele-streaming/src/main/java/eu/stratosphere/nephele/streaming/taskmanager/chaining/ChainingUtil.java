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

import eu.stratosphere.nephele.streaming.message.action.ChainTasksAction;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.RuntimeChain;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.RuntimeChainLink;

/**
 * @author bjoern
 * 
 */
public class ChainingUtil {

	public static void chainTaskThreads(TaskChain chainModel)
			throws InterruptedException {

		ArrayList<RuntimeChainLink> chainLinks = new ArrayList<RuntimeChainLink>();
		for (int i = 0; i < chainModel.getNumberOfChainedTasks(); i++) {
			StreamTaskEnvironment taskEnvironment = chainModel.getTask(i)
					.getStreamTaskEnvironment();
			chainLinks.add(new RuntimeChainLink(taskEnvironment.getMapper(),
					taskEnvironment.getInputGate(0), taskEnvironment
							.getOutputGate(0)));
		}

		RuntimeChain runtimeChain = new RuntimeChain(chainLinks);
		chainLinks.get(0).getOutputGate()
				.enqueueQosAction(new ChainTasksAction(runtimeChain));
		runtimeChain.waitUntilTasksAreChained();
	}

	public static void unchainTaskThreads(TaskChain leftChain,
			TaskChain rightChain) {

		// FIXME
	}
}
