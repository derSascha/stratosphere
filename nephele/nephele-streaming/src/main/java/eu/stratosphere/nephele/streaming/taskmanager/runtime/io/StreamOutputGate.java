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

import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.plugins.wrapper.AbstractOutputGateWrapper;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener.OutputGateQosReportingListener;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.StreamChain;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

public final class StreamOutputGate<T extends Record> extends
		AbstractOutputGateWrapper<T> {

	// private long lastThroughputTimestamp = -1L;
	//
	// private long[] lastSentBytes = null;

	private StreamChain streamChain = null;

	private OutputGateQosReportingListener qosCallback;

	public StreamOutputGate(final OutputGate<T> wrappedOutputGate) {
		super(wrappedOutputGate);
	}

	public void setQosReportingCallback(
			OutputGateQosReportingListener qosCallback) {
		this.qosCallback = qosCallback;
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

	private void handlePendingQosActions() throws InterruptedException {
		if (this.qosCallback != null) {
			this.qosCallback.handlePendingQosActions();
		}
	}

	public void reportRecordEmitted(final T record) {
		if (this.qosCallback != null) {
			AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
			int outputChannel = getChannelSelector().selectChannels(record,
					getNumberOfOutputChannels())[0];

			this.qosCallback.recordEmitted(outputChannel, taggableRecord);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void outputBufferSent(final int channelIndex) {
		if (this.qosCallback != null) {
			this.qosCallback.outputBufferSent(channelIndex);
		}
		this.getWrappedOutputGate().outputBufferSent(channelIndex);
	}

	public void redirectToStreamChain(final StreamChain streamChain) {
		this.streamChain = streamChain;
	}

	@Override
	public void initializeCompressors() throws CompressionException {
		this.getWrappedOutputGate().initializeCompressors();
	}
}
