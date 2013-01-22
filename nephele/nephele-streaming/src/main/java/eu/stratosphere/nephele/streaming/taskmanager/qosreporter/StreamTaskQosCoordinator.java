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
package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.streaming.message.action.AbstractAction;
import eu.stratosphere.nephele.streaming.message.action.ConstructStreamChainAction;
import eu.stratosphere.nephele.streaming.message.action.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.TaskLatencyReporter.TaskType;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener.QosReportingListenerManager;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.StreamChain;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.StreamChainCoordinator;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamInputGate;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamOutputGate;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class StreamTaskQosCoordinator {

	private static final Log LOG = LogFactory
			.getLog(StreamTaskQosCoordinator.class);

	private ExecutionVertexID vertexID;

	private StreamTaskEnvironment taskEnvironment;

	private QosReporterThread reporterThread;

	private RecordTagger recordTagger;

	private ChannelLatencyReporter channelLatencyReporter;

	private TaskLatencyReporter taskLatencyReporter;

	private LinkedBlockingQueue<AbstractAction> pendingActions;

	private StreamChainCoordinator chainCoordinator;
	
	private ChannelOutputBufferLifetimeReporter bufferLatencyReporter;
	
	private ChannelLookupHelper channelLookup;
	
	public StreamTaskQosCoordinator(ExecutionVertexID vertexID,
			StreamTaskEnvironment taskEnvironment, QosReporterThread reporterThread,
			StreamChainCoordinator chainCoordinator) {

		this.vertexID = vertexID;
		this.taskEnvironment = taskEnvironment;
		this.reporterThread = reporterThread;
		this.chainCoordinator = chainCoordinator;
		this.pendingActions = new LinkedBlockingQueue<AbstractAction>();
		
		registerTaskAsChainMapperIfNecessary();
		
		// FIXME taskType stuff is actually incorrect. depends not on task
		// type
		// but on the profiling sequence and whether input channels need to
		// be profiled
		this.taskLatencyReporter = new TaskLatencyReporter(vertexID,
				determineTaskType(), reporterThread);
		
		this.channelLookup = new ChannelLookupHelper();
		populateChannelLookupAndRegisterQosListeners();				
	}

	private void registerTaskAsChainMapperIfNecessary() {
		if(this.taskEnvironment.isMapperTask()) {
			this.chainCoordinator.registerMapper(this.taskEnvironment);
		}
	}

	private void populateChannelLookupAndRegisterQosListeners() {
		boolean vertexNeedsProfiling = this.reporterThread.needsQosReporting(this.vertexID);
		 
		boolean inputListenerInstalled = installInputGateListeners(vertexNeedsProfiling);
		if (inputListenerInstalled) {
			// FIXME
		}

		boolean outputListenerInstalled = installOutputGateListeners(vertexNeedsProfiling);
		if (inputListenerInstalled) {
			// FIXME
		}
	}
	
	public boolean installInputGateListeners(boolean vertexNeedsProfiling) {

		boolean listenerInstalled = false;
		for (StreamInputGate<?> gate : this.taskEnvironment.getInputGates()) {

			int noOfChannels = gate.getNumberOfInputChannels();
			if (noOfChannels <= 0) {
				LOG.warn("Cannot register QoS reporting listener on input gate without channels");
				continue;
			}

			// assumption: QoS constraints are defined on the job graph level.
			// Hence, if one channel of a gate needs QoS reporting, all other
			// channels of the gate need it too.
			boolean qosReportingRequired = this.reporterThread
					.needsQosReporting(gate.getInputChannel(0)
							.getConnectedChannelID());

			if (qosReportingRequired) {
				int inputGateIndex = this.channelLookup.registerInputGate(gate);
				QosReportingListenerManager.installInputGateListener(gate,
						this, inputGateIndex, vertexNeedsProfiling);
				listenerInstalled = true;
				// FIXME all this code here currently only works for ONE input gate
				break; 
			}
		}
		
		return listenerInstalled;
	}


	private boolean installOutputGateListeners(boolean vertexNeedsProfiling) {
		
		boolean outputListenerInstalled = false;
		for (StreamOutputGate<?> gate : this.taskEnvironment.getOutputGates()) {

			int noOfChannels = gate.getNumberOfOutputChannels();
			if (noOfChannels <= 0) {
				LOG.warn("Cannot register QoS reporting listener on output gate without channels");
				continue;
			}

			// assumption: QoS constraints are defined on the job graph level.
			// Hence, if one channel of a gate needs QoS reporting, all other
			// channels of the gate need it too.
			boolean qosReportingRequired = this.reporterThread
					.needsQosReporting(gate.getOutputChannel(0).getID());

			if (qosReportingRequired) {
				int outputGateIndex = this.channelLookup.registerChannelsFromOutputGate(gate);
				QosReportingListenerManager.installOutputGateListener(gate,
						this, outputGateIndex, vertexNeedsProfiling);
				outputListenerInstalled = true;
				// FIXME all this code here currently only works for ONE output gate
				break; 
			}
		}
		return outputListenerInstalled;
	}


	private TaskType determineTaskType() {
		if (this.taskEnvironment.getNumberOfInputGates() == 0) {
			return TaskType.INPUT;
		} else if (this.taskEnvironment.getNumberOfOutputGates() == 0) {
			return TaskType.OUTPUT;
		} else {
			return TaskType.REGULAR;
		}
	}

	public void recordReceived(int inputGateIndex, int inputChannelIndex, Record record) {
		TimestampTag timestampTag = (TimestampTag) ((AbstractTaggableRecord) record).getTag();
		
		if (timestampTag != null) {
			ChannelID outputChannelID = this.channelLookup.getInputChannel(
					inputGateIndex, inputChannelIndex).getConnectedChannelID();
			this.channelLatencyReporter.reportLatencyIfNecessary(inputChannelIndex, outputChannelID, timestampTag);
		}
	}
	
	public void taskReadsRecord() {
		this.taskLatencyReporter.processRecordReceived();
	}
	
	public void taskEmitsRecord() {
		this.taskLatencyReporter.processRecordEmitted();
	}

	public void recordEmitted(int outputGateIndex, int outputChannelIndex, AbstractTaggableRecord record) {
		this.recordTagger.tagRecordIfNecessary(outputChannelIndex, record);
	}
	
	public void outputBufferSent(int outputGateIndex, int outputChannelIndex) {
		ChannelID outputChannelID = this.channelLookup.getOutputChannel(
				outputGateIndex, outputChannelIndex).getID();
		this.bufferLatencyReporter.outputBufferSent(outputChannelIndex,
				outputChannelID);
	}

	public void queueAction(AbstractAction action) {
		this.pendingActions.add(action);
	}

	public void executePendingQosActions() throws InterruptedException {
		AbstractAction action;
		while ((action = this.pendingActions.poll()) != null) {
			if (action instanceof LimitBufferSizeAction) {
				this.limitBufferSize((LimitBufferSizeAction) action);
			} else if (action instanceof ConstructStreamChainAction) {
				this.constructStreamChain((ConstructStreamChainAction) action);
			} else {
				LOG.error("Ignoring unknown action of type "
						+ action.getClass());
			}
		}
	}

	private void constructStreamChain(ConstructStreamChainAction csca)
			throws InterruptedException {

		StreamChain streamChain = this.chainCoordinator
				.constructStreamChain(csca.getVertexIDs());
		streamChain.waitUntilFlushed();
	}

	
	private void limitBufferSize(LimitBufferSizeAction lbsa) {

		ChannelID sourceChannelID = lbsa.getSourceChannelID();
		int bufferSize = lbsa.getBufferSize();
		
		AbstractByteBufferedOutputChannel<?> channel = this.channelLookup
				.getOutputChannelByID(sourceChannelID);
		if (channel == null) {
			LOG.error("Cannot find output channel with ID " + sourceChannelID);
			return;
		}

		LOG.info("Setting buffer size limit of output channel "
				+ sourceChannelID + " to " + bufferSize + " bytes");
		channel.limitBufferSize(bufferSize);
	}
}
