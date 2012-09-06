package eu.stratosphere.nephele.streaming.wrappers;

import java.util.HashMap;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.streaming.StreamingTag;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.streaming.types.ChannelLatency;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;

public class InputGateChannelLatencyReporter {

	private StreamListener streamListener;

	private HashMap<ExecutionVertexID, ChannelLatencyInfo> channelLatencyInfos = new HashMap<ExecutionVertexID, ChannelLatencyInfo>();

	private class ChannelLatencyInfo {
		long timeOfLastReportSentToJobManager;

		long timeOfLastReceivedTag;

		long accumulatedLatency;

		int tagsReceived;

		public boolean reportToJobManagerIsDue() {
			return (timeOfLastReceivedTag - timeOfLastReportSentToJobManager > streamListener.getContext().getAggregationInterval())
				&& tagsReceived > 0;
		}

		public void reset(long now) {
			timeOfLastReportSentToJobManager = now;
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

			if (latencyInfo.reportToJobManagerIsDue()) {
				doReport(tag, now, latencyInfo);
				reportSent = true;
			}
		}

		return reportSent;
	}
	
	private void doReport(StreamingTag tag, long now, ChannelLatencyInfo latencyInfo) {
		ChannelLatency channelLatency = new ChannelLatency(streamListener.getContext().getJobID(),
			tag.getSourceID(),
			streamListener.getContext().getVertexID(),
			latencyInfo.accumulatedLatency / latencyInfo.tagsReceived);

		streamListener.reportChannelLatency(channelLatency);
		latencyInfo.reset(now);
	}

	private ChannelLatencyInfo processTagLatency(StreamingTag tag, long now) {
		ChannelLatencyInfo info = channelLatencyInfos.get(tag.getSourceID());

		if (info == null) {
			info = new ChannelLatencyInfo();
			info.timeOfLastReportSentToJobManager = now;
			info.accumulatedLatency = 0;
			info.tagsReceived = 0;
			channelLatencyInfos.put(tag.getSourceID(), info);
		}

		info.timeOfLastReceivedTag = now;
		info.accumulatedLatency += now - tag.getTimestamp();
		info.tagsReceived++;

		return info;
	}

}
