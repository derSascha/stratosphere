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
import java.util.concurrent.atomic.AtomicBoolean;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.RecordAvailabilityListener;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkInputChannel;
import eu.stratosphere.nephele.plugins.wrapper.AbstractInputGateWrapper;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener.InputGateQosReportingListener;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

/**
 * Wraps Nephele's {@link eu.stratosphere.nephele.io.RuntimeInputGate} to
 * intercept methods calls necessary for Qos statistics collection.
 * 
 * @author Bjoern Lohrmann
 * 
 * @param <T>
 */
public final class StreamInputGate<T extends Record> extends
		AbstractInputGateWrapper<T> {

	private final InputChannelChooser channelChooser;

	private HashMap<ChannelID, AbstractInputChannel<T>> inputChannels;

	/**
	 * The thread which executes the task connected to the input gate.
	 */
	private Thread executingThread = null;

	private AtomicBoolean taskThreadHalted = new AtomicBoolean(false);

	private volatile InputGateQosReportingListener qosCallback;

	public StreamInputGate(final InputGate<T> wrappedInputGate) {
		super(wrappedInputGate);
		this.channelChooser = new InputChannelChooser();
		this.inputChannels = new HashMap<ChannelID, AbstractInputChannel<T>>();
	}

	public void setQosReportingListener(
			InputGateQosReportingListener qosCallback) {
		this.qosCallback = qosCallback;
	}

	public InputGateQosReportingListener getQosReportingListener() {
		return this.qosCallback;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T readRecord(final T target) throws IOException,
			InterruptedException {
		if (this.executingThread == null) {
			this.executingThread = Thread.currentThread();
		}

		if (this.isClosed()) {
			return null;
		}

		if (this.executingThread.isInterrupted()) {
			throw new InterruptedException();
		}

		T record = null;
		int channelToReadFrom;
		do {
			channelToReadFrom = this.channelChooser
					.chooseNextAvailableChannel();

			if (channelToReadFrom != -1) {
				// regular reading
				record = this.readFromChannel(channelToReadFrom, target);
			} else {
				// never returns and dumps any incoming data
				this.trapTaskThread(target);
			}
		} while (record == null);

		this.reportRecordReceived(record, channelToReadFrom);

		return record;
	}

	private T readFromChannel(int channelToReadFrom, T targetRecord)
			throws IOException {
		final T returnValue = this.getInputChannel(channelToReadFrom)
				.readRecord(targetRecord);

		if (returnValue == null) {
			this.channelChooser.markCurrentChannelUnavailable();
		}

		return returnValue;
	}

	/**
	 * @param record
	 *            The record that has been received.
	 * @param sourceChannelID
	 *            The ID of the source channel (output channel)
	 */
	public void reportRecordReceived(Record record, int inputChannel) {
		if (this.qosCallback != null) {
			AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
			this.qosCallback.recordReceived(inputChannel, taggableRecord);
		}
	}

	/**
	 * This method should only be called if this input gate is inside a chain
	 * and the task thread (doing this call) should therefore be halted (unless
	 * interrupted).
	 * 
	 * @param target
	 * @throws InterruptedException
	 *             if task thread is interrupted.
	 */
	private void trapTaskThread(T target) throws InterruptedException {
		this.signalThatTaskHasHalted();

		// this should never return unless the task is interrupted
		this.dumpIncomingData(target);

		throw new RuntimeException(
				"Failed to halt chained task, this is a bug.");
	}

	private void dumpIncomingData(T target) throws InterruptedException {
		this.channelChooser.setBlockIfNoChannelAvailable(true);
		while (true) {
			int channelToDump = this.channelChooser
					.chooseNextAvailableChannel();
			try {
				// FIXME: this silently dumps data that was in transit at the
				// time of task locking
				// also this MAY be unsafe if invoking target.read() multiple
				// times throws exceptions
				while (this.getInputChannel(channelToDump).readRecord(target) != null) {
				}
			} catch (IOException e) {
			}
		}
	}

	private void signalThatTaskHasHalted() {
		synchronized (this.taskThreadHalted) {
			this.taskThreadHalted.set(true);
			this.taskThreadHalted.notify();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void notifyRecordIsAvailable(final int channelIndex) {
		this.channelChooser.addIncomingAvailableChannel(channelIndex);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerRecordAvailabilityListener(
			final RecordAvailabilityListener<T> listener) {
		this.getWrappedInputGate().registerRecordAvailabilityListener(listener);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasRecordAvailable() throws IOException,
			InterruptedException {
		return this.getWrappedInputGate().hasRecordAvailable();
	}

	@Override
	public void notifyDataUnitConsumed(int channelIndex) {
		this.getWrappedInputGate().notifyDataUnitConsumed(channelIndex);
	}

	public void haltTaskThread() throws InterruptedException {
		this.channelChooser.setBlockIfNoChannelAvailable(false);
		synchronized (this.taskThreadHalted) {
			if (!this.taskThreadHalted.get()) {
				this.taskThreadHalted.wait();
			}
		}
	}

	public AbstractInputChannel<? extends Record> getInputChannel(
			ChannelID channelID) {
		return this.inputChannels.get(channelID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkInputChannel<T> createNetworkInputChannel(
			final InputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID) {

		NetworkInputChannel<T> channel = this.getWrappedInputGate()
				.createNetworkInputChannel(inputGate, channelID,
						connectedChannelID);

		this.inputChannels.put(channelID, channel);

		return channel;

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InMemoryInputChannel<T> createInMemoryInputChannel(
			final InputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID) {

		InMemoryInputChannel<T> channel = this.getWrappedInputGate()
				.createInMemoryInputChannel(inputGate, channelID,
						connectedChannelID);

		this.inputChannels.put(channelID, channel);
		return channel;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void removeAllInputChannels() {
		this.inputChannels.clear();
		this.getWrappedInputGate().removeAllInputChannels();
	}
}
