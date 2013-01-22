package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.message.profiling.ChannelThroughput;

public class ChannelThroughputReporter {

	private ChannelThroughputInfo[] channelThroughputInfos;

	private QosReporterThread qosReporter;

	private class ChannelThroughputInfo {

		long timeOfLastReport;
		
		long amountTransmittedAtLastReport;
		
		long amountTransmittedSinceLastReport;

		public boolean reportIsDue(long now) {
			return now - this.timeOfLastReport > ChannelThroughputReporter.this.qosReporter
					.getConfiguration().getAggregationInterval()
					&& this.amountTransmittedSinceLastReport > 0;
		}

		public void reset(long now, long currentAmountTransmitted) {
			this.timeOfLastReport = now;
			this.amountTransmittedAtLastReport = currentAmountTransmitted;
			this.amountTransmittedSinceLastReport = 0;
		}
		
		public void update(long currentAmountTransmitted) {
			this.amountTransmittedSinceLastReport = currentAmountTransmitted
					- this.amountTransmittedAtLastReport;
		}
	}

	public ChannelThroughputReporter(QosReporterThread qosReporter, int noOfOutputChannels) {
		this.qosReporter = qosReporter;
		this.channelThroughputInfos = new ChannelThroughputInfo[noOfOutputChannels];
	}

	public boolean reportThroughputIfNecessary(int channelIndex, ChannelID channelID,
			long currentAmountTransmitted) {
		
		boolean reportSent = false;
		long now = System.currentTimeMillis();

		ChannelThroughputInfo throughputInfo = this.channelThroughputInfos[channelIndex];
		if (throughputInfo == null) {
			throughputInfo = new ChannelThroughputInfo();
			throughputInfo.timeOfLastReport = System.currentTimeMillis();
			throughputInfo.amountTransmittedAtLastReport = currentAmountTransmitted;
			throughputInfo.amountTransmittedSinceLastReport = 0;
		} else {
			throughputInfo.update(currentAmountTransmitted);

			if (throughputInfo.reportIsDue(now)) {
				this.sendReport(throughputInfo, now, channelID);
				reportSent = true;
				throughputInfo.reset(now, currentAmountTransmitted);
			}
		}

		return reportSent;
	}

	private void sendReport(ChannelThroughputInfo throughputInfo, long now,
			ChannelID channelID) {
		
		double secsPassed = (now - throughputInfo.timeOfLastReport) / 1000.0;
		double mbitTransmitted = (throughputInfo.amountTransmittedSinceLastReport * 8) / 1000000.0;

		ChannelThroughput channelThroughput = new ChannelThroughput(channelID,
				mbitTransmitted / secsPassed);
		this.qosReporter.addToNextReport(channelThroughput);
	}
}
