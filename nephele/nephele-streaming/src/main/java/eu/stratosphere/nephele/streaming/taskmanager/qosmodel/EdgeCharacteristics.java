package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import eu.stratosphere.nephele.streaming.message.profiling.OutputChannelStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.BufferSizeHistory;

public class EdgeCharacteristics {

	private ProfilingEdge edge;

	private ProfilingValueStatistic latencyInMillisStatistic;

	private ProfilingValueStatistic throughputInMbitStatistic;

	private ProfilingValueStatistic outputBufferLifetimeStatistic;
	
	private ProfilingValueStatistic recordsPerBufferStatistic;
	
	private ProfilingValueStatistic recordsPerSecondStatistic;

	private BufferSizeHistory bufferSizeHistory;
	
	private final static int DEFAULT_NO_OF_STATISTICS_ENTRIES = 4;

	private boolean isInChain;

	public EdgeCharacteristics(ProfilingEdge edge, int noOfStatisticsEntries) {
		this.edge = edge;
		this.isInChain = false;
		this.latencyInMillisStatistic = new ProfilingValueStatistic(noOfStatisticsEntries);
		this.throughputInMbitStatistic = new ProfilingValueStatistic(noOfStatisticsEntries);
		this.outputBufferLifetimeStatistic = new ProfilingValueStatistic(noOfStatisticsEntries);
		this.recordsPerBufferStatistic = new ProfilingValueStatistic(noOfStatisticsEntries);
		this.recordsPerSecondStatistic = new ProfilingValueStatistic(noOfStatisticsEntries);
		this.bufferSizeHistory = new BufferSizeHistory(2);
	}
	
	public EdgeCharacteristics(ProfilingEdge edge) {
		this(edge, DEFAULT_NO_OF_STATISTICS_ENTRIES);
	}

	public ProfilingEdge getEdge() {
		return this.edge;
	}

	public double getChannelLatencyInMillis() {
		if (this.latencyInMillisStatistic.hasValues()) {
			return this.latencyInMillisStatistic.getArithmeticMean();
		}

		return -1;
	}

	public double getChannelThroughputInMbit() {
		if (this.throughputInMbitStatistic.hasValues()) {
			return this.throughputInMbitStatistic.getArithmeticMean();
		}
		return -1;
	}

	public double getOutputBufferLifetimeInMillis() {
		if (this.isInChain()) {
			return 0;
		}
		if (this.outputBufferLifetimeStatistic.hasValues()) {
			return this.outputBufferLifetimeStatistic.getArithmeticMean();
		}
		return -1;
	}
	
	public double getRecordsPerBuffer() {
		if (this.recordsPerBufferStatistic.hasValues()) {
			return this.recordsPerBufferStatistic.getArithmeticMean();
		}
		return -1;
	}

	public double getRecordsPerSecond() {
		if (this.recordsPerSecondStatistic.hasValues()) {
			return this.recordsPerSecondStatistic.getArithmeticMean();
		}
		return -1;
	}

	public void addLatencyMeasurement(long timestamp, double latencyInMillis) {
		ProfilingValue value = new ProfilingValue(latencyInMillis, timestamp);
		this.latencyInMillisStatistic.addValue(value);
	}

	public void addOutputChannelStatisticsMeasurement(long timestamp,
			OutputChannelStatistics stats) {
		
		ProfilingValue throughput = new ProfilingValue(stats.getThroughput(),
				timestamp);
		this.throughputInMbitStatistic.addValue(throughput);

		ProfilingValue outputBufferLifetime = new ProfilingValue(
				stats.getOutputBufferLifetime(), timestamp);
		this.outputBufferLifetimeStatistic.addValue(outputBufferLifetime);

		ProfilingValue recordsPerBuffer = new ProfilingValue(
				stats.getRecordsPerBuffer(), timestamp);
		this.recordsPerBufferStatistic.addValue(recordsPerBuffer);

		ProfilingValue recordsPerSecond = new ProfilingValue(
				stats.getRecordsPerSecond(), timestamp);
		this.recordsPerSecondStatistic.addValue(recordsPerSecond);
	}

	public boolean isChannelLatencyFresherThan(long freshnessThreshold) {
		return this.latencyInMillisStatistic.getOldestValue().getTimestamp() >= freshnessThreshold;
	}

	public boolean isOutputBufferLifetimeFresherThan(long freshnessThreshold) {
		if (this.isInChain()) {
			return true;
		}
		return this.outputBufferLifetimeStatistic.getOldestValue()
				.getTimestamp() >= freshnessThreshold;
	}

	public void setIsInChain(boolean isInChain) {
		this.isInChain = isInChain;
	}

	public boolean isInChain() {
		return this.isInChain;
	}

	public boolean isActive() {
		return this.latencyInMillisStatistic.hasValues()
				&& (this.isInChain() || this.outputBufferLifetimeStatistic
						.hasValues());
	}

	public BufferSizeHistory getBufferSizeHistory() {
		return this.bufferSizeHistory;
	}

	public int getBufferSize() {
		return this.bufferSizeHistory.getLastEntry().getBufferSize();
	}
}
