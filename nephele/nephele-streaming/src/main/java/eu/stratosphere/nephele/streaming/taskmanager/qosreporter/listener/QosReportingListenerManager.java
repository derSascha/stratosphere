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
package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener;

import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.StreamTaskQosCoordinator;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamInputGate;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamOutputGate;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class QosReportingListenerManager {

	public static void installInputGateListener(StreamInputGate<?> inputGate,
			final StreamTaskQosCoordinator taskQosCoordinator,
			final int inputGateIndex,
			final boolean vertexNeedsProfiling) {

		inputGate.setQosReportingListener(new InputGateQosReportingListener() {
			@Override
			public void recordReceived(int inputChannelIndex, AbstractTaggableRecord record) {		
				
				taskQosCoordinator.recordReceived(
							inputGateIndex,
							inputChannelIndex,
							record);
				
				if (vertexNeedsProfiling) {
					taskQosCoordinator.taskReadsRecord();
				}
			}
		});
	}
	
	public static void installOutputGateListener(StreamOutputGate<?> outputGate,
			final StreamTaskQosCoordinator taskQosCoordinator,
			final int outputGateIndex,
			final boolean vertexNeedsProfiling) {
		

		outputGate.setQosReportingCallback(new OutputGateQosReportingListener() {
			@Override
			public void recordEmitted(int outputChannelIndex,
					AbstractTaggableRecord record) {

				if (vertexNeedsProfiling) {
					taskQosCoordinator.taskEmitsRecord();
				}
				
				taskQosCoordinator.recordEmitted(outputGateIndex, outputChannelIndex, record);
			}

			@Override
			public void outputBufferSent(int outputChannelIndex) {
				taskQosCoordinator.outputBufferSent(outputGateIndex, outputChannelIndex);
			}

			@Override
			public void handlePendingQosActions() throws InterruptedException {
				taskQosCoordinator.executePendingQosActions();
			}
		});
	}
}
