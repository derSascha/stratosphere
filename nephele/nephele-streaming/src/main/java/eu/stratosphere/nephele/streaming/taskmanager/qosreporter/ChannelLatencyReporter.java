package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import java.util.ArrayList;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.message.profiling.ChannelLatency;

public class ChannelLatencyReporter {

	private ArrayList<ChannelLatencyInfo> channelLatencyInfos;

	private QosReporterThread qosReporter;

	private class ChannelLatencyInfo {

		long timeOfLastReport;

		long timeOfLastReceivedTag;

		long accumulatedLatency;

		int tagsReceived;

		public boolean reportIsDue() {
			return this.timeOfLastReceivedTag - this.timeOfLastReport > ChannelLatencyReporter.this.qosReporter
					.getConfiguration().getAggregationInterval()
					&& this.tagsReceived > 0;
		}

		public void reset(long now) {
			this.timeOfLastReport = now;
			this.accumulatedLatency = 0;
			this.tagsReceived = 0;
		}
		
		public void update(TimestampTag tag, long now) {
			this.timeOfLastReceivedTag = now;
			this.accumulatedLatency += now - tag.getTimestamp();
			this.tagsReceived++;
		}
	}

	public ChannelLatencyReporter(QosReporterThread qosReporter, int noOfInputChannels) {
		this.qosReporter = qosReporter;
		this.channelLatencyInfos = new ArrayList<ChannelLatencyInfo>();
		populateChannelLatencies(noOfInputChannels);
	}

	private void populateChannelLatencies(int noOfInputChannels) {
		long now = System.currentTimeMillis();
		for (int i = 0; i < noOfInputChannels; i++) {
			ChannelLatencyInfo info = new ChannelLatencyInfo();
			info.timeOfLastReport = now;
			info.accumulatedLatency = 0;
			info.tagsReceived = 0;
			this.channelLatencyInfos.add(info);	
		}
	}

	public boolean reportLatencyIfNecessary(int channelIndex, ChannelID channelID,
			TimestampTag timestampTag) {
		
		boolean reportSent = false;
		long now = System.currentTimeMillis();

		ChannelLatencyInfo latencyInfo = this.channelLatencyInfos.get(channelIndex);
		latencyInfo.update(timestampTag, now);

		if (latencyInfo.reportIsDue()) {
			this.sendReport(latencyInfo, channelID);
			reportSent = true;
			latencyInfo.reset(now);
		}

		return reportSent;
	}

	private void sendReport(ChannelLatencyInfo latencyInfo, ChannelID channelID) {
		ChannelLatency channelLatency = new ChannelLatency(channelID,
				latencyInfo.accumulatedLatency / latencyInfo.tagsReceived);
		this.qosReporter.addToNextReport(channelLatency);
	}
}
