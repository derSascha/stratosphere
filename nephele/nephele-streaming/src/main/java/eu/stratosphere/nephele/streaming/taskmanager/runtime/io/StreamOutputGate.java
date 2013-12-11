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

package eu.stratosphere.nephele.streaming.taskmanager.runtime.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkOutputChannel;
import eu.stratosphere.nephele.plugins.wrapper.AbstractOutputGateWrapper;
import eu.stratosphere.nephele.streaming.message.action.AbstractQosAction;
import eu.stratosphere.nephele.streaming.message.action.ConstructStreamChainAction;
import eu.stratosphere.nephele.streaming.message.action.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener.OutputGateQosReportingListener;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.StreamChain;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

/**
 * Wraps Nephele's {@link eu.stratosphere.nephele.io.RuntimeOutputGate} to
 * intercept methods calls necessary for Qos statistics collection.
 * 
 * @author Bjoern Lohrmann
 * 
 * @param <T>
 */
public final class StreamOutputGate<T extends Record> extends
		AbstractOutputGateWrapper<T> {

	private final static Logger LOG = Logger.getLogger(StreamOutputGate.class);

	private StreamChain streamChain = null;

	private volatile OutputGateQosReportingListener qosCallback;

	private HashMap<ChannelID, AbstractOutputChannel<T>> outputChannels;

	private StreamChannelSelector<T> streamChannelSelector;

	private LinkedBlockingQueue<AbstractQosAction> qosActionQueue;

	public StreamOutputGate(final OutputGate<T> wrappedOutputGate,
			StreamChannelSelector<T> streamChannelSelector) {
		super(wrappedOutputGate);
		this.outputChannels = new HashMap<ChannelID, AbstractOutputChannel<T>>();
		this.streamChannelSelector = streamChannelSelector;
		this.qosActionQueue = new LinkedBlockingQueue<AbstractQosAction>();
	}

	public void setQosReportingListener(
			OutputGateQosReportingListener qosCallback) {
		this.qosCallback = qosCallback;
	}

	public OutputGateQosReportingListener getQosReportingListener() {
		return this.qosCallback;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(final T record) throws IOException,
			InterruptedException {

		this.reportRecordEmitted(record);

		if (this.streamChain == null) {
			this.getWrappedOutputGate().writeRecord(record);
		} else {
			this.streamChain.writeRecord(record);
		}

		this.handlePendingQosActions();
	}

	public void enqueueQosAction(AbstractQosAction qosAction) {
		this.qosActionQueue.add(qosAction);
	}

	private void handlePendingQosActions() throws InterruptedException {
		AbstractQosAction action;
		while ((action = this.qosActionQueue.poll()) != null) {
			if (action instanceof LimitBufferSizeAction) {
				this.limitBufferSize((LimitBufferSizeAction) action);
			} else if (action instanceof ConstructStreamChainAction) {
				this.constructStreamChain((ConstructStreamChainAction) action);
			}
		}
	}

	public AbstractOutputChannel<T> getOutputChannel(ChannelID channelID) {
		return this.outputChannels.get(channelID);
	}

	private void constructStreamChain(ConstructStreamChainAction csca)
			throws InterruptedException {

		// FIXME
		// StreamChain streamChain = this.chainCoordinator
		// .constructStreamChain(csca.getVertexIDs());
		// streamChain.waitUntilFlushed();
	}

	private void limitBufferSize(LimitBufferSizeAction lbsa) {
		ChannelID channelID = lbsa.getSourceChannelID();

		AbstractByteBufferedOutputChannel<T> channel = (AbstractByteBufferedOutputChannel<T>) this.outputChannels
				.get(channelID);

		if (channel == null) {
			LOG.error("Cannot find output channel with ID " + channelID);
			return;
		}

		channel.limitBufferSize(lbsa.getBufferSize());
	}

	public void reportRecordEmitted(final T record) {
		int outputChannel = this.streamChannelSelector
				.invokeWrappedChannelSelector(record,
						this.getNumberOfOutputChannels())[0];

		if (this.qosCallback != null) {
			AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
			this.qosCallback.recordEmitted(outputChannel, taggableRecord);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void outputBufferSent(final int channelIndex) {
		if (this.qosCallback != null) {
			this.qosCallback.outputBufferSent(channelIndex, this
					.getOutputChannel(channelIndex)
					.getAmountOfDataTransmitted());
		}
		this.getWrappedOutputGate().outputBufferSent(channelIndex);
	}

	public void redirectToStreamChain(final StreamChain streamChain) {
		this.streamChain = streamChain;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkOutputChannel<T> createNetworkOutputChannel(
			final OutputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID) {

		NetworkOutputChannel<T> channel = this.getWrappedOutputGate()
				.createNetworkOutputChannel(inputGate, channelID,
						connectedChannelID);

		this.outputChannels.put(channelID, channel);

		return channel;

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InMemoryOutputChannel<T> createInMemoryOutputChannel(
			final OutputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID) {

		InMemoryOutputChannel<T> channel = this.getWrappedOutputGate()
				.createInMemoryOutputChannel(inputGate, channelID,
						connectedChannelID);

		this.outputChannels.put(channelID, channel);
		return channel;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelSelector<T> getChannelSelector() {
		return this.streamChannelSelector;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void removeAllOutputChannels() {
		this.outputChannels.clear();
		this.getWrappedOutputGate().removeAllOutputChannels();
	}
}
