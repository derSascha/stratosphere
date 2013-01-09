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

package eu.stratosphere.nephele.streaming.chaining;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.Mapper;
import eu.stratosphere.nephele.streaming.wrappers.StreamingOutputGate;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public final class StreamChain {

	private static final Log LOG = LogFactory.getLog(StreamChain.class);

	private final List<StreamChainLink<?, ?>> chainLinks;

	StreamChain(final List<StreamChainLink<?, ?>> chainLinks) {

		if (chainLinks.isEmpty()) {
			throw new IllegalArgumentException(
					"List chainLinks must not be empty");
		}

		this.chainLinks = chainLinks;
	}

	public StreamingOutputGate<? extends Record> getFirstOutputGate() {

		return this.chainLinks.get(0).getOutputGate();
	}

	public void writeRecord(final Record record) throws IOException {

		try {
			this.executeMapper(record, 1);
		} catch (Exception e) {
			throw new IOException(StringUtils.stringifyException(e));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	void executeMapper(final Record record, final int chainIndex)
			throws Exception {

		final StreamChainLink chainLink = this.chainLinks.get(chainIndex);
		final Mapper mapper = chainLink.getMapper();

		chainLink.getInputGate().reportRecordReceived(record);
		mapper.map(record);

		final StreamingOutputGate outputGate = chainLink.getOutputGate();

		final Queue outputCollector = mapper.getOutputCollector();

		if (chainIndex == this.chainLinks.size() - 1) {

			while (!outputCollector.isEmpty()) {
				outputGate.writeRecord((Record) outputCollector.poll());
			}

		} else {

			while (!outputCollector.isEmpty()) {
				final Record outputRecord = (Record) outputCollector.poll();
				outputGate.reportRecordEmitted(outputRecord);
				this.executeMapper(RecordUtils.createCopy(outputRecord),
						chainIndex + 1);
			}
		}
	}

	public void waitUntilFlushed() throws InterruptedException {
		try {
			LOG.info("Locking task threads in chain");
			for (int i = 0; i < this.chainLinks.size(); i++) {
				StreamChainLink<?, ?> chainLink = this.chainLinks.get(i);
				if (i == 0) {
					chainLink.getOutputGate().flush();
					chainLink.getOutputGate().redirectToStreamChain(this);
				} else {
					chainLink.getInputGate().haltTaskThread();
					chainLink.getOutputGate().flush();
				}
			}
			LOG.info("Task threads chain in successfully locked");
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
		}

	}
}
