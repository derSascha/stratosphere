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

import eu.stratosphere.nephele.streaming.message.action.DropCurrentChainAction;
import eu.stratosphere.nephele.streaming.message.action.EstablishNewChainAction;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.RuntimeChain;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.RuntimeChainLink;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class ChainingUtil {

	public static void chainTaskThreads(TaskChain chainModel)
			throws InterruptedException {

		RuntimeChain runtimeChain = assembleRuntimeChain(chainModel);
		runtimeChain.getFirstOutputGate().enqueueQosAction(
				new EstablishNewChainAction(runtimeChain));
		runtimeChain.waitUntilTasksAreChained();
	}

	private static RuntimeChain assembleRuntimeChain(TaskChain chainModel) {
		ArrayList<RuntimeChainLink> chainLinks = new ArrayList<RuntimeChainLink>();
		for (int i = 0; i < chainModel.getNumberOfChainedTasks(); i++) {
			StreamTaskEnvironment taskEnvironment = chainModel.getTask(i)
					.getStreamTaskEnvironment();
			chainLinks.add(new RuntimeChainLink(taskEnvironment,
					taskEnvironment.getInputGate(0), taskEnvironment
							.getOutputGate(0)));
		}

		RuntimeChain runtimeChain = new RuntimeChain(chainLinks);
		return runtimeChain;
	}

	public static void unchainTaskThreads(TaskChain leftChain,
			TaskChain rightChain) throws InterruptedException {

		if (leftChain.getNumberOfChainedTasks() > 1) {
			RuntimeChain leftRuntimeChain = assembleRuntimeChain(leftChain);
			leftRuntimeChain.getFirstOutputGate().enqueueQosAction(
					new EstablishNewChainAction(leftRuntimeChain));
			leftRuntimeChain.waitUntilTasksAreChained();
		} else {
			leftChain.getTask(0).getStreamTaskEnvironment().getOutputGate(0)
					.enqueueQosAction(new DropCurrentChainAction());
		}

		if (rightChain.getNumberOfChainedTasks() > 1) {
			RuntimeChain rightRuntimeChain = assembleRuntimeChain(rightChain);
			rightRuntimeChain.getFirstOutputGate().enqueueQosAction(
					new EstablishNewChainAction(rightRuntimeChain));
			rightRuntimeChain.getFirstInputGate().wakeUpTaskThreadIfNecessary();
			rightRuntimeChain.waitUntilTasksAreChained();
		} else {
			rightChain.getTask(0).getStreamTaskEnvironment().getInputGate(0).wakeUpTaskThreadIfNecessary();
		}
	}
}
