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

package eu.stratosphere.nephele.streaming.wrappers;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.RecordAvailabilityListener;
import eu.stratosphere.nephele.io.channels.AbstractChannel;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.plugins.wrapper.AbstractInputGateWrapper;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

public final class StreamingInputGate<T extends Record> extends AbstractInputGateWrapper<T> {

	private final StreamListener streamListener;

	/**
	 * Queue with indices of channels that store at least one available record.
	 */
	private final ArrayDeque<Integer> availableChannels = new ArrayDeque<Integer>();

	/**
	 * The channel to read from next.
	 */
	private int channelToReadFrom = -1;

	/**
	 * The value returned by the last call of waitForAnyChannelToBecomeAvailable
	 */
	private int availableChannelRetVal = -1;

	/**
	 * The thread which executes the task connected to the input gate.
	 */
	private Thread executingThread = null;

	private InputGateChannelLatencyReporter channelLatencyReporter;

	StreamingInputGate(final InputGate<T> wrappedInputGate, final StreamListener streamListener) {
		super(wrappedInputGate);

		if (streamListener == null) {
			throw new IllegalArgumentException("Argument streamListener must not be null");
		}

		this.streamListener = streamListener;
		this.channelLatencyReporter = new InputGateChannelLatencyReporter(streamListener);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T readRecord(final T target) throws IOException, InterruptedException {

		T record = null;

		if (this.executingThread == null) {
			this.executingThread = Thread.currentThread();
		}

		if (this.executingThread.isInterrupted()) {
			throw new InterruptedException();
		}

		final int numberOfInputChannels = getNumberOfInputChannels();

		while (record == null) {
			if (this.channelToReadFrom == -1) {
				this.availableChannelRetVal = waitForAnyChannelToBecomeAvailable();
				this.channelToReadFrom = this.availableChannelRetVal;
			}
			try {
				record = this.getInputChannel(this.channelToReadFrom).readRecord(target);
			} catch (EOFException e) {
				// System.out.println("### Caught EOF exception at channel " + channelToReadFrom + "(" +
				// this.getInputChannel(channelToReadFrom).getType().toString() + ")");
				if (this.isClosed()) {
					return null;
				}
			}

			if (record == null && this.channelToReadFrom == this.availableChannelRetVal) {
				this.channelToReadFrom = -1;
			} else {
				this.channelToReadFrom++;
				if (this.channelToReadFrom == numberOfInputChannels) {
					this.channelToReadFrom = 0;
				}
			}
		}

		reportRecordReceived(record);

		return record;
	}

	/**
	 * @param record
	 *        The record that has been received.
	 * @param sourceChannelID
	 *        The ID of the source channel (output channel)
	 */
	public void reportRecordReceived(final Record record) {
		this.streamListener.recordReceived(record);
		this.channelLatencyReporter.reportLatencyIfNecessary((AbstractTaggableRecord) record);
	}

	/**
	 * This method returns the index of a channel which has at least
	 * one record available. The method may block until at least one
	 * channel has become ready.
	 * 
	 * @return the index of the channel which has at least one record available
	 */
	public int waitForAnyChannelToBecomeAvailable() throws InterruptedException {

		synchronized (this.availableChannels) {

			while (this.availableChannels.isEmpty()) {
				this.availableChannels.wait();
			}

			return this.availableChannels.removeFirst().intValue();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void notifyRecordIsAvailable(final int channelIndex) {

		synchronized (this.availableChannels) {

			this.availableChannels.add(Integer.valueOf(channelIndex));
			this.availableChannels.notify();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerRecordAvailabilityListener(final RecordAvailabilityListener<T> listener) {
		getWrappedInputGate().registerRecordAvailabilityListener(listener);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasRecordAvailable() throws IOException, InterruptedException {
		return getWrappedInputGate().hasRecordAvailable();
	}

	@Override
	public void notifyDataUnitConsumed(int channelIndex) {
		getWrappedInputGate().notifyDataUnitConsumed(channelIndex);
	}

	@Override
	public void initializeDecompressors() throws CompressionException {
		getWrappedInputGate().initializeDecompressors();
	}
}
