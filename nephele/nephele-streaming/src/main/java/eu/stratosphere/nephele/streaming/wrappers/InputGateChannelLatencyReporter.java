package eu.stratosphere.nephele.streaming.wrappers;

import java.util.HashMap;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.StreamingTag;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelLatency;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;

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
			return (timeOfLastReceivedTag - timeOfLastReport > streamListener.getContext()
				.getAggregationInterval())
				&& tagsReceived > 0;
		}

		public void reset(long now) {
			timeOfLastReport = now;
			accumulatedLatency = 0;
			tagsReceived = 0;
		}
	}

	public InputGateChannelLatencyReporter(StreamListener streamListener) {
		this.streamListener = streamListener;
	}

	public boolean reportLatencyIfNecessary(AbstractTaggableRecord record) {
		boolean reportSent = false;

		StreamingTag tag = (StreamingTag) record.getTag();
		if (tag != null) {
			long now = System.currentTimeMillis();

			ChannelLatencyInfo latencyInfo = processTagLatency(tag, now);

			if (latencyInfo.reportIsDue()) {
				doReport(now, latencyInfo);
				reportSent = true;
			}
		}

		return reportSent;
	}

	private void doReport(long now, ChannelLatencyInfo latencyInfo) {
		ChannelLatency channelLatency = new ChannelLatency(latencyInfo.sourceChannelID,
			latencyInfo.accumulatedLatency / latencyInfo.tagsReceived);

		streamListener.reportChannelLatency(channelLatency);
		latencyInfo.reset(now);
	}

	private ChannelLatencyInfo processTagLatency(StreamingTag tag, long now) {
		ChannelLatencyInfo info = channelLatencyInfos.get(tag.getSourceChannelID());

		if (info == null) {
			info = new ChannelLatencyInfo();
			info.sourceChannelID = tag.getSourceChannelID();
			info.timeOfLastReport = now;
			info.accumulatedLatency = 0;
			info.tagsReceived = 0;
			channelLatencyInfos.put(tag.getSourceChannelID(), info);
		}

		info.timeOfLastReceivedTag = now;
		info.accumulatedLatency += now - tag.getTimestamp();
		info.tagsReceived++;

		return info;
	}

}
