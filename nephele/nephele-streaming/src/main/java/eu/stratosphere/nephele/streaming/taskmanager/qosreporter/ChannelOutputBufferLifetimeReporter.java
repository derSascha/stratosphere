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

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.message.profiling.OutputBufferLatency;

/**
 * @author Daniel Warneke, Bjoern Lohrmann
 * 
 */
public class ChannelOutputBufferLifetimeReporter {

	private OutputBufferLifetimeInfo[] bufferLifetimes;

	private QosReporterThread qosReporter;

	private class OutputBufferLifetimeInfo {

		public int buffersSent;

		public long timeOfLastReport;

		public boolean isReportDue(long now) {
			return now - this.timeOfLastReport > ChannelOutputBufferLifetimeReporter.this.qosReporter
					.getConfiguration().getAggregationInterval()
					&& this.buffersSent > 0;
		}

		public void reset(long now) {
			this.buffersSent = 0;
			this.timeOfLastReport = now;
		}
	}

	private ChannelOutputBufferLifetimeReporter(QosReporterThread qosReporter,
			int noOfOutputChannels) {
		this.qosReporter = qosReporter;
		this.bufferLifetimes = new OutputBufferLifetimeInfo[noOfOutputChannels];
		populateBufferLifetimes();
	}

	private void populateBufferLifetimes() {
		long now = System.currentTimeMillis();
		for (int i = 0; i < this.bufferLifetimes.length; i++) {
			OutputBufferLifetimeInfo bufferLifetime = new OutputBufferLifetimeInfo();
			bufferLifetime.buffersSent = 0;
			bufferLifetime.timeOfLastReport = now;
		}
	}

	public void outputBufferSent(int channelIndex, ChannelID outputChannelID) {
		final long now = System.currentTimeMillis();

		OutputBufferLifetimeInfo bufferLifetime = this.bufferLifetimes[channelIndex];
		bufferLifetime.buffersSent++;
		
		if(bufferLifetime.isReportDue(now)) {
			double avgLifetime = (now - bufferLifetime.timeOfLastReport) / ((double) bufferLifetime.buffersSent); 
			this.qosReporter.addToNextReport(new OutputBufferLatency(outputChannelID, avgLifetime));
			bufferLifetime.reset(now);
		}
	}
}
