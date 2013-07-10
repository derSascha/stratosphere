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
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.streaming.message.action.AbstractAction;
import eu.stratosphere.nephele.streaming.message.action.ConstructStreamChainAction;
import eu.stratosphere.nephele.streaming.message.action.EdgeQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.action.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.message.action.VertexQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.qosreport.DummyVertexReporterActivity;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener.QosReportingListenerHelper;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.StreamChain;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.StreamChainCoordinator;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamInputGate;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamOutputGate;
import eu.stratosphere.nephele.types.Record;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class StreamTaskQosCoordinator implements QosReporterConfigListener {

	private static final Log LOG = LogFactory
			.getLog(StreamTaskQosCoordinator.class);

	private ExecutionVertexID vertexID;

	private StreamTaskEnvironment taskEnvironment;

	private QosReportForwarderThread reporterThread;

	/**
	 * For each input gate of the task for whose channels latency reporting is
	 * required, this list contains a InputGateReporterManager. A
	 * InputGateReporterManager keeps track of and reports on the latencies for
	 * all of the input gate's channels. This is a sparse list (may contain
	 * nulls), indexed by the runtime gate's own indices.
	 */
	private ArrayList<InputGateReporterManager> inputGateReporters;

	/**
	 * For each output gate of the task for whose output channels QoS statistics
	 * are required (throughput, output buffer lifetime, ...), this list
	 * contains a OutputGateReporterManager. Each OutputGateReporterManager
	 * keeps track of and reports on Qos statistics all of the output gate's
	 * channels and also attaches tags to records sent via its channels. This is
	 * a sparse list (may contain nulls), indexed by the runtime gate's own
	 * indices.
	 */
	private ArrayList<OutputGateReporterManager> outputGateReporters;

	/**
	 * For each input/output gate combination for which Qos reports are
	 * required, this VertexLatencyReportManager creates the reports.
	 */
	private VertexLatencyReportManager vertexLatencyManager;

	private LinkedBlockingQueue<AbstractAction> pendingActions;

	private StreamChainCoordinator chainCoordinator;

	private QosReporterConfigCenter reporterConfigCenter;

	public StreamTaskQosCoordinator(ExecutionVertexID vertexID,
			StreamTaskEnvironment taskEnvironment,
			QosReportForwarderThread reportForwarder,
			StreamChainCoordinator chainCoordinator) {

		this.vertexID = vertexID;
		this.taskEnvironment = taskEnvironment;
		this.reporterThread = reportForwarder;
		this.chainCoordinator = chainCoordinator;
		this.pendingActions = new LinkedBlockingQueue<AbstractAction>();
		this.reporterConfigCenter = reportForwarder.getConfigCenter();

		this.vertexLatencyManager = new VertexLatencyReportManager(
				this.reporterThread,
				this.taskEnvironment.getNumberOfInputGates(),
				this.taskEnvironment.getNumberOfOutputGates());
		this.inputGateReporters = new ArrayList<InputGateReporterManager>();
		this.outputGateReporters = new ArrayList<OutputGateReporterManager>();

		prepareQosReporting();
		registerTaskAsChainMapperIfNecessary();
	}

	private void registerTaskAsChainMapperIfNecessary() {
		if (this.taskEnvironment.isMapperTask()) {
			this.chainCoordinator.registerMapper(this.taskEnvironment);
		}
	}

	private void prepareQosReporting() {
		installVertexLatencyReporters();
		installInputGateListeners();
		installOutputGateListeners();
	}

	private void installVertexLatencyReporters() {
		Set<VertexQosReporterConfig> vertexReporterConfigs = this.reporterConfigCenter
				.getVertexQosReporters(this.vertexID);

		if (vertexReporterConfigs.isEmpty()) {
			this.reporterConfigCenter.setQosReporterConfigListener(
					this.vertexID, this);
		} else {
			for (VertexQosReporterConfig reporterConfig : vertexReporterConfigs) {
				if (reporterConfig.isDummy()) {
					announceDummyReporter(reporterConfig.getReporterID());
				} else {
					installVertexLatencyReporter(reporterConfig);
				}
			}
		}
	}

	private void announceDummyReporter(QosReporterID.Vertex reporterID) {
		this.reporterThread.addToNextReport(new DummyVertexReporterActivity(
				reporterID));
	}

	private void installVertexLatencyReporter(
			VertexQosReporterConfig reporterConfig) {

		QosReporterID.Vertex reporterID = reporterConfig.getReporterID();

		if (this.vertexLatencyManager.containsReporter(reporterID)) {
			return;
		}

		StreamInputGate<? extends Record> inputGate = this.taskEnvironment
				.getInputGate(reporterConfig.getInputGateID());

		StreamOutputGate<? extends Record> outputGate = this.taskEnvironment
				.getOutputGate(reporterConfig.getOutputGateID());

		this.vertexLatencyManager.addReporter(inputGate.getIndex(),
				outputGate.getIndex(), reporterID);

		QosReportingListenerHelper.listenToVertexLatencyOnInputGate(inputGate,
				this.vertexLatencyManager);
		QosReportingListenerHelper.listenToVertexLatencyOnOutputGate(
				outputGate, this.vertexLatencyManager);
	}

	private void installInputGateListeners() {
		for (int i = 0; i < this.taskEnvironment.getNumberOfInputGates(); i++) {
			StreamInputGate<? extends Record> inputGate = this.taskEnvironment
					.getInputGate(i);

			// as constraints are defined on job graph level, it is safe to only
			// test one channel
			boolean mustReportQosForGate = this.reporterConfigCenter
					.getEdgeQosReporter(inputGate.getInputChannel(0)
							.getConnectedChannelID()) != null;

			if (!mustReportQosForGate) {
				this.inputGateReporters.add(null);
				this.reporterConfigCenter.setQosReporterConfigListener(
						inputGate.getGateID(), this);
				break;
			}

			InputGateReporterManager reporter = new InputGateReporterManager(
					this.reporterThread, inputGate.getNumberOfInputChannels());
			this.inputGateReporters.add(reporter);

			for (int j = 0; j < inputGate.getNumberOfInputChannels(); j++) {
				int runtimeChannelIndex = inputGate.getInputChannel(j)
						.getChannelIndex();
				ChannelID sourceChannelID = inputGate.getInputChannel(j)
						.getConnectedChannelID();

				EdgeQosReporterConfig edgeReporter = this.reporterConfigCenter
						.getEdgeQosReporter(sourceChannelID);
				QosReporterID.Edge reporterID = (QosReporterID.Edge) edgeReporter
						.getReporterID();
				reporter.addEdgeQosReporterConfig(runtimeChannelIndex,
						reporterID);
			}

			QosReportingListenerHelper.listenToChannelLatenciesOnInputGate(
					inputGate, reporter);
		}
	}

	private void installOutputGateListeners() {
		for (int i = 0; i < this.taskEnvironment.getNumberOfOutputGates(); i++) {
			StreamOutputGate<? extends Record> outputGate = this.taskEnvironment
					.getOutputGate(i);

			// as constraints are defined on job graph level, it is safe to only
			// test one channel
			boolean mustReportQosForGate = this.reporterConfigCenter
					.getEdgeQosReporter(outputGate.getOutputChannel(0).getID()) != null;

			if (!mustReportQosForGate) {
				this.outputGateReporters.add(null);
				this.reporterConfigCenter.setQosReporterConfigListener(
						outputGate.getGateID(), this);
				break;
			}

			OutputGateReporterManager gateReporterManager = new OutputGateReporterManager(
					this.reporterThread, outputGate.getNumberOfOutputChannels());

			this.outputGateReporters.add(gateReporterManager);

			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); j++) {
				int runtimeChannelIndex = outputGate.getOutputChannel(j)
						.getChannelIndex();
				ChannelID sourceChannelID = outputGate.getOutputChannel(j)
						.getID();

				EdgeQosReporterConfig edgeReporter = this.reporterConfigCenter
						.getEdgeQosReporter(sourceChannelID);
				QosReporterID.Edge reporterID = (QosReporterID.Edge) edgeReporter
						.getReporterID();
				gateReporterManager.addEdgeQosReporterConfig(
						runtimeChannelIndex, reporterID);
			}

			QosReportingListenerHelper
					.listenToOutputChannelStatisticsOnOutputGate(outputGate,
							gateReporterManager, this);
		}
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

		ChannelID channelID = lbsa.getSourceChannelID();

		AbstractByteBufferedOutputChannel<?> channel = retrieveOutputChannel(channelID);
		if (channel == null) {
			LOG.error("Cannot find output channel with ID " + channelID);
			return;
		}

		int bufferSize = lbsa.getBufferSize();
		LOG.info("Setting buffer size limit of output channel " + channelID
				+ " to " + bufferSize + " bytes");
		channel.limitBufferSize(bufferSize);
	}

	private AbstractByteBufferedOutputChannel<?> retrieveOutputChannel(
			ChannelID sourceChannelID) {

		for (int i = 0; i < this.taskEnvironment.getNumberOfOutputGates(); i++) {
			AbstractOutputChannel<?> channel = this.taskEnvironment
					.getOutputGate(i).getOutputChannel(sourceChannelID);

			if (channel != null) {
				return (AbstractByteBufferedOutputChannel<?>) channel;
			}
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.nephele.streaming.taskmanager.qosreporter.
	 * QosReporterConfigListener
	 * #newVertexQosReporter(eu.stratosphere.nephele.streaming
	 * .message.action.VertexQosReporterConfig)
	 */
	@Override
	public void newVertexQosReporter(VertexQosReporterConfig reporterConfig) {
		if (reporterConfig.isDummy()) {
			announceDummyReporter(reporterConfig.getReporterID());
		} else {
			installVertexLatencyReporter(reporterConfig);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.nephele.streaming.taskmanager.qosreporter.
	 * QosReporterConfigListener
	 * #newEdgeQosReporter(eu.stratosphere.nephele.streaming
	 * .message.action.EdgeQosReporterConfig)
	 */
	@Override
	public void newEdgeQosReporter(EdgeQosReporterConfig edgeReporter) {
		QosReporterID.Edge reporterID = (QosReporterID.Edge) edgeReporter
				.getReporterID();

		StreamInputGate<? extends Record> inputGate = this.taskEnvironment
				.getInputGate(edgeReporter.getInputGateID());
		if (inputGate != null) {
			int runtimeGateIndex = inputGate.getIndex();
			int runtimeChannelIndex = inputGate.getInputChannel(
					edgeReporter.getTargetChannelID()).getChannelIndex();

			if (this.inputGateReporters.get(runtimeGateIndex) == null) {
				createAndRegisterGateReporterManager(inputGate);
			}

			this.inputGateReporters.get(runtimeGateIndex)
					.addEdgeQosReporterConfig(runtimeChannelIndex, reporterID);
		} else {
			StreamOutputGate<? extends Record> outputGate = this.taskEnvironment
					.getOutputGate(edgeReporter.getOutputGateID());

			int runtimeGateIndex = outputGate.getIndex();
			int runtimeChannelIndex = outputGate.getOutputChannel(
					edgeReporter.getSourceChannelID()).getChannelIndex();

			if (this.outputGateReporters.get(runtimeGateIndex) == null) {
				createAndRegisterGateReporterManager(outputGate);
			}

			this.outputGateReporters.get(runtimeGateIndex)
					.addEdgeQosReporterConfig(runtimeChannelIndex, reporterID);
		}
	}

	private void createAndRegisterGateReporterManager(
			StreamInputGate<? extends Record> inputGate) {

		int runtimeGateIndex = inputGate.getIndex();
		InputGateReporterManager gateReporterManager = new InputGateReporterManager(
				this.reporterThread, inputGate.getNumberOfInputChannels());

		this.inputGateReporters.set(runtimeGateIndex, gateReporterManager);

		QosReportingListenerHelper.listenToChannelLatenciesOnInputGate(
				inputGate, gateReporterManager);
	}

	private void createAndRegisterGateReporterManager(
			StreamOutputGate<? extends Record> outputGate) {

		int runtimeGateIndex = outputGate.getIndex();

		OutputGateReporterManager gateReporterManager = new OutputGateReporterManager(
				this.reporterThread, outputGate.getNumberOfOutputChannels());

		this.outputGateReporters.set(runtimeGateIndex, gateReporterManager);

		QosReportingListenerHelper.listenToOutputChannelStatisticsOnOutputGate(
				outputGate, gateReporterManager, this);
	}
}
