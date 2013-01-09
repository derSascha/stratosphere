package eu.stratosphere.nephele.streaming.wrappers;

import java.util.HashMap;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.TimestampTag;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelLatency;

public class InputGateChannelLatencyReporter {

	private StreamListener streamListener;

	/**
	 * Channel latencies by source channel ID.
	 */
	private HashMap<ChannelID, ChannelLatencyInfo> channelLatencyInfos = new HashMap<ChannelID, ChannelLatencyInfo>();

	private class ChannelLatencyInfo {

		ChannelID sourceChannelID;

		long timeOfLastReport;

		long timeOfLastReceivedTag;

		long accumulatedLatency;

		int tagsReceived;

		public boolean reportIsDue() {
			return this.timeOfLastReceivedTag - this.timeOfLastReport > InputGateChannelLatencyReporter.this.streamListener
					.getContext().getAggregationInterval()
					&& this.tagsReceived > 0;
		}

		public void reset(long now) {
			this.timeOfLastReport = now;
			this.accumulatedLatency = 0;
			this.tagsReceived = 0;
		}
	}

	public InputGateChannelLatencyReporter(StreamListener streamListener) {
		this.streamListener = streamListener;
	}

	public boolean reportLatencyIfNecessary(TimestampTag timestampTag,
			ChannelID sourceChannel) {
		boolean reportSent = false;

		long now = System.currentTimeMillis();

		ChannelLatencyInfo latencyInfo = this.processTagLatency(timestampTag,
				sourceChannel, now);

		if (latencyInfo.reportIsDue()) {
			this.doReport(now, latencyInfo);
			reportSent = true;
		}

		return reportSent;
	}

	private void doReport(long now, ChannelLatencyInfo latencyInfo) {
		ChannelLatency channelLatency = new ChannelLatency(
				latencyInfo.sourceChannelID, latencyInfo.accumulatedLatency
						/ latencyInfo.tagsReceived);

		this.streamListener.reportChannelLatency(channelLatency);
		latencyInfo.reset(now);
	}

	private ChannelLatencyInfo processTagLatency(TimestampTag tag,
			ChannelID sourceChannel, long now) {
		
		ChannelLatencyInfo info = this.channelLatencyInfos.get(sourceChannel);

		if (info == null) {
			info = new ChannelLatencyInfo();
			info.sourceChannelID = sourceChannel;
			info.timeOfLastReport = now;
			info.accumulatedLatency = 0;
			info.tagsReceived = 0;
			this.channelLatencyInfos.put(sourceChannel, info);
		}

		info.timeOfLastReceivedTag = now;
		info.accumulatedLatency += now - tag.getTimestamp();
		info.tagsReceived++;

		return info;
	}

}
