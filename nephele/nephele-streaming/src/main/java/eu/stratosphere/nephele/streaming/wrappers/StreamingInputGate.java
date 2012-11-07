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
import java.util.concurrent.atomic.AtomicBoolean;

import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.RecordAvailabilityListener;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.plugins.wrapper.AbstractInputGateWrapper;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

public final class StreamingInputGate<T extends Record> extends
		AbstractInputGateWrapper<T> {

	private final StreamListener streamListener;

	private final InputChannelChooser channelChooser = new InputChannelChooser();

	/**
	 * The thread which executes the task connected to the input gate.
	 */
	private Thread executingThread = null;

	private InputGateChannelLatencyReporter channelLatencyReporter;

	private AtomicBoolean taskThreadHalted = new AtomicBoolean(false);

	StreamingInputGate(final InputGate<T> wrappedInputGate,
			final StreamListener streamListener) {
		super(wrappedInputGate);

		if (streamListener == null) {
			throw new IllegalArgumentException(
					"Argument streamListener must not be null");
		}

		this.streamListener = streamListener;
		this.channelLatencyReporter = new InputGateChannelLatencyReporter(
				streamListener);
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

		if (isClosed()) {
			return null;
		}

		if (this.executingThread.isInterrupted()) {
			throw new InterruptedException();
		}

		T record = null;
		do {
			int channelToReadFrom = this.channelChooser
					.chooseNextAvailableChannel();

			if (channelToReadFrom != -1) {
				// regular reading
				record = readFromChannel(channelToReadFrom, target);
			} else {
				// never returns and dumps any incoming data
				trapTaskThread(target);
			}
		} while (record == null);

		reportRecordReceived(record);

		return record;
	}

	private T readFromChannel(int channelToReadFrom, T targetRecord)
			throws IOException {
		T returnValue = null;
		try {
			returnValue = this.getInputChannel(channelToReadFrom).readRecord(
					targetRecord);
		} catch (EOFException e) {
		}
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
	public void reportRecordReceived(final Record record) {
		this.streamListener.recordReceived(record);
		this.channelLatencyReporter
				.reportLatencyIfNecessary((AbstractTaggableRecord) record);
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
		signalThatTaskHasHalted();

		// this should never return unless the task is interrupted
		dumpIncomingData(target);

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
				// also this MAY be unsafe if invoking target.read() multipled
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
		getWrappedInputGate().registerRecordAvailabilityListener(listener);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasRecordAvailable() throws IOException,
			InterruptedException {
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

	public void haltTaskThread() throws InterruptedException {
		this.channelChooser.setBlockIfNoChannelAvailable(false);
		synchronized (this.taskThreadHalted) {
			if (!this.taskThreadHalted.get()) {
				this.taskThreadHalted.wait();
			}
		}
	}
}
