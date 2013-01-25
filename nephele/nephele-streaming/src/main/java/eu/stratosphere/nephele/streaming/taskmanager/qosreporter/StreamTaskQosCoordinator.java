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

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.streaming.message.action.AbstractAction;
import eu.stratosphere.nephele.streaming.message.action.ConstructStreamChainAction;
import eu.stratosphere.nephele.streaming.message.action.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener.QosReportingListenerHelper;
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

	/**
	 * For each input gate of the task for whose channels latency reporting
	 * is required, this list contains a latency reporter. Such a latency reporter
	 * keeps reports latencies for all of the input gate's channels.
	 */
	private ArrayList<ChannelLatencyReporter> inputGateReporters;
	
	/**
	 * For each output gate of the task for whose output channels QoS statistics
	 * are required (throughput, output buffer lifetime, ...), this list
	 * contains a statistics reporter. Each statistics reporter reports for all
	 * of the output gate's channels.
	 */
	private ArrayList<OutputChannelStatisticsReporter> outputGateReporters;	

	/**
	 * For each output gate of the task for whose output channels QoS statistics
	 * are required (such as latency), this list contains a record tagger. Each
	 * record tagger manages the tag of the output gate's channels. Tags are
	 * used to mark perform latency measurements on the channel.
	 */
	private ArrayList<RecordTagger> recordTaggers;

	private TaskLatencyReporter taskLatencyReporter;

	private LinkedBlockingQueue<AbstractAction> pendingActions;

	private StreamChainCoordinator chainCoordinator;
	
	private ChannelLookupHelper channelLookup;
	
	public StreamTaskQosCoordinator(ExecutionVertexID vertexID,
			StreamTaskEnvironment taskEnvironment, QosReporterThread reporterThread,
			StreamChainCoordinator chainCoordinator) {

		this.vertexID = vertexID;
		this.taskEnvironment = taskEnvironment;
		this.reporterThread = reporterThread;
		this.chainCoordinator = chainCoordinator;
		this.pendingActions = new LinkedBlockingQueue<AbstractAction>();
		
		this.inputGateReporters = new ArrayList<ChannelLatencyReporter>();
		this.outputGateReporters = new ArrayList<OutputChannelStatisticsReporter>();
		this.recordTaggers = new ArrayList<RecordTagger>();	
		this.channelLookup = new ChannelLookupHelper();
		
		prepareQosReporting();
		registerTaskAsChainMapperIfNecessary();
	}

	private void registerTaskAsChainMapperIfNecessary() {
		if(this.taskEnvironment.isMapperTask()) {
			this.chainCoordinator.registerMapper(this.taskEnvironment);
		}
	}

	private void prepareQosReporting() {
		boolean vertexNeedsProfiling = this.reporterThread.needsQosReporting(this.vertexID);
		if (vertexNeedsProfiling) {		
			this.taskLatencyReporter = new TaskLatencyReporter(this.vertexID,
					this.reporterThread);
		}
		
		installInputGateListeners(vertexNeedsProfiling);
		installOutputGateListeners(vertexNeedsProfiling);
	}
	
	private void installInputGateListeners(boolean vertexNeedsProfiling) {
		for (int i = 0; i < this.taskEnvironment.getNumberOfInputGates(); i++) {

			StreamInputGate<?> gate = this.taskEnvironment.getInputGate(i);

			// assumption: QoS constraints are defined on the job graph level.
			// Hence, if one channel of a gate needs QoS reporting, all other
			// channels of the gate need it too.
			boolean qosReportingRequired = this.reporterThread
					.needsQosReporting(gate.getInputChannel(0)
							.getConnectedChannelID());

			if (qosReportingRequired) {
				installInputGateListener(gate, vertexNeedsProfiling);
			}
		}
	}

	private void installInputGateListener(StreamInputGate<?> gate, boolean vertexNeedsProfiling) {
		int noOfChannels = gate.getNumberOfInputChannels();
		if (noOfChannels <= 0) {
			LOG.warn("Cannot register QoS reporting listener on input gate without channels");
			return;
		}
		
		int inputGateIndex = this.channelLookup.registerInputGate(gate);
		
		if(vertexNeedsProfiling) {
			QosReportingListenerHelper.installInputGateListenerWithVertexProfiling(gate, this, inputGateIndex);					
		} else {
			QosReportingListenerHelper.installInputGateListenerWithoutVertexProfiling(gate, this, inputGateIndex);
		}
	
		this.inputGateReporters.add(new ChannelLatencyReporter(this.reporterThread, noOfChannels));
	}


	private void installOutputGateListeners(boolean vertexNeedsProfiling) {
		
		for (int i = 0; i < this.taskEnvironment.getNumberOfOutputGates(); i++) {

			StreamOutputGate<?> gate = this.taskEnvironment.getOutputGate(i);

			// assumption: QoS constraints are defined on the job graph level.
			// Hence, if one channel of a gate needs QoS reporting, all other
			// channels of the gate need it too.
			boolean qosReportingRequired = this.reporterThread
					.needsQosReporting(gate.getOutputChannel(0).getID());

			if (qosReportingRequired) {
				installOutputGateListener(gate, vertexNeedsProfiling);
			}
		}
	}

	private void installOutputGateListener(StreamOutputGate<?> gate,
			boolean vertexNeedsProfiling) {
		
		int noOfChannels = gate.getNumberOfOutputChannels();
		if (noOfChannels <= 0) {
			LOG.warn("Cannot register QoS reporting listener on output gate without channels");
			return;
		}
		
		int outputGateIndex = this.channelLookup.registerChannelsFromOutputGate(gate);
		
		if(vertexNeedsProfiling) {
			QosReportingListenerHelper.installOutputGateListenerWithVertexProfiling(gate,
					this, outputGateIndex);					
		} else {
			QosReportingListenerHelper.installOutputGateListenerWithoutVertexProfiling(gate,
					this, outputGateIndex);										
		}
		
		this.outputGateReporters.add(new OutputChannelStatisticsReporter(
						this.reporterThread, noOfChannels));
		this.recordTaggers.add(new RecordTagger(this.reporterThread,
				noOfChannels));
	}

	public void recordReceived(int inputGateIndex, int inputChannelIndex, Record record) {
		TimestampTag timestampTag = (TimestampTag) ((AbstractTaggableRecord) record).getTag();
		
		if (timestampTag != null) {
			ChannelID outputChannelID = this.channelLookup.getInputChannel(
					inputGateIndex, inputChannelIndex).getConnectedChannelID();
			
			this.inputGateReporters.get(inputGateIndex).reportLatencyIfNecessary(inputChannelIndex,
							outputChannelID, timestampTag);
		}
	}
	
	public void taskReadsRecord() {
		this.taskLatencyReporter.processRecordReceived();
	}
	
	public void taskEmitsRecord() {
		this.taskLatencyReporter.processRecordEmitted();
	}

	public void recordEmitted(int outputGateIndex, int outputChannelIndex, AbstractTaggableRecord record) {
		this.recordTaggers.get(outputGateIndex).tagRecordIfNecessary(outputChannelIndex, record);
		this.outputGateReporters.get(outputGateIndex).recordEmitted(outputChannelIndex);
	}
	
	public void outputBufferSent(int outputGateIndex, int outputChannelIndex) {
		AbstractByteBufferedOutputChannel<?> channel = this.channelLookup
				.getOutputChannel(outputGateIndex, outputChannelIndex);
		this.outputGateReporters.get(outputGateIndex).outputBufferSent(outputChannelIndex,
				channel.getID(), channel.getAmountOfDataTransmitted());
	}

	public void queueQosAction(AbstractAction action) {
		this.pendingActions.add(action);
	}

	public void executeQueuedQosActions() throws InterruptedException {
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
