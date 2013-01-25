package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.message.profiling.OutputChannelStatistics;

public class OutputChannelStatisticsReporter {

	private ChannelStatistics[] channelStatistics;

	private QosReporterThread qosReporter;

	private class ChannelStatistics {

		long timeOfLastReport;

		long amountTransmittedAtLastReport;

		long currentAmountTransmitted;

		int recordsEmittedSinceLastReport;

		int outputBuffersSentSinceLastReport;

		public boolean reportIsDue(long now) {
			return now - this.timeOfLastReport > OutputChannelStatisticsReporter.this.qosReporter
					.getConfiguration().getAggregationInterval()
					&& this.recordsEmittedSinceLastReport > 0
					&& this.outputBuffersSentSinceLastReport > 0;
		}

		public void reset(long now) {
			this.timeOfLastReport = now;
			this.amountTransmittedAtLastReport = this.currentAmountTransmitted;
			this.recordsEmittedSinceLastReport = 0;
			this.outputBuffersSentSinceLastReport = 0;
		}
	}

	public OutputChannelStatisticsReporter(QosReporterThread qosReporter,
			int noOfOutputChannels) {
		this.qosReporter = qosReporter;
		this.channelStatistics = new ChannelStatistics[noOfOutputChannels];
	}

	public void recordEmitted(int channelIndex) {
		this.getChannelStatistics(channelIndex).recordsEmittedSinceLastReport++;
	}

	public void outputBufferSent(int channelIndex, ChannelID outputChannelID,
			long currentAmountTransmitted) {

		ChannelStatistics channelStats = this
				.getChannelStatistics(channelIndex);
		channelStats.outputBuffersSentSinceLastReport++;
		channelStats.currentAmountTransmitted = currentAmountTransmitted;

		sendReportIfNecessary(channelStats, outputChannelID);
	}

	private ChannelStatistics getChannelStatistics(int channelIndex) {
		ChannelStatistics channelStats = this.channelStatistics[channelIndex];
		if (channelStats == null) {
			channelStats = new ChannelStatistics();
			channelStats.timeOfLastReport = System.currentTimeMillis();
			channelStats.amountTransmittedAtLastReport = 0;
			channelStats.currentAmountTransmitted = 0;
			channelStats.recordsEmittedSinceLastReport = 0;
			this.channelStatistics[channelIndex] = channelStats;
		}

		return channelStats;
	}

	private boolean sendReportIfNecessary(ChannelStatistics channelStats,
			ChannelID channelID) {

		boolean reportSent = false;
		long now = System.currentTimeMillis();

		if (channelStats.reportIsDue(now)) {
			this.sendReport(channelStats, now, channelID);
			reportSent = true;
			channelStats.reset(now);
		}

		return reportSent;
	}

	private void sendReport(ChannelStatistics stats, long now,
			ChannelID channelID) {

		double secsPassed = (now - stats.timeOfLastReport) / 1000.0;
		double mbitPerSec = ((stats.currentAmountTransmitted - stats.amountTransmittedAtLastReport) * 8)
				/ (1000000.0 * secsPassed);
		long outputBufferLifetime = (now - stats.timeOfLastReport) / stats.outputBuffersSentSinceLastReport;
		double recordsPerBuffer = ((double) stats.recordsEmittedSinceLastReport / stats.outputBuffersSentSinceLastReport);
		double recordsPerSecond = stats.recordsEmittedSinceLastReport
				/ secsPassed;

		OutputChannelStatistics channelStatsMessage = new OutputChannelStatistics(
				channelID, mbitPerSec, outputBufferLifetime, recordsPerBuffer,
				recordsPerSecond);
		this.qosReporter.addToNextReport(channelStatsMessage);
	}
}
