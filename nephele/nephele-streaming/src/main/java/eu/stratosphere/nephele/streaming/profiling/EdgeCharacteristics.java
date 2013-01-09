package eu.stratosphere.nephele.streaming.profiling;

import eu.stratosphere.nephele.streaming.buffers.BufferSizeHistory;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingEdge;

public class EdgeCharacteristics {

	private ProfilingEdge edge;

	private ProfilingValueStatistic latencyInMillisStatistic;

	private ProfilingValueStatistic throughputInMbitStatistic;

	private ProfilingValueStatistic outputBufferLifetimeStatistic;

	private BufferSizeHistory bufferSizeHistory;

	private boolean isInChain;

	public EdgeCharacteristics(ProfilingEdge edge) {
		this.edge = edge;
		this.isInChain = false;
		this.latencyInMillisStatistic = new ProfilingValueStatistic(5);
		this.throughputInMbitStatistic = new ProfilingValueStatistic(5);
		this.outputBufferLifetimeStatistic = new ProfilingValueStatistic(5);
		this.bufferSizeHistory = new BufferSizeHistory(2);
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

	public void addLatencyMeasurement(long timestamp, double latencyInMillis) {
		ProfilingValue value = new ProfilingValue(latencyInMillis, timestamp);
		this.latencyInMillisStatistic.addValue(value);
	}

	public void addThroughputMeasurement(long timestamp, double throughputInMbit) {
		ProfilingValue value = new ProfilingValue(throughputInMbit, timestamp);
		this.throughputInMbitStatistic.addValue(value);
	}

	public void addOutputBufferLatencyMeasurement(long timestamp,
			double latencyInMillis) {
		ProfilingValue value = new ProfilingValue(latencyInMillis, timestamp);
		this.outputBufferLifetimeStatistic.addValue(value);
	}

	public boolean isChannelLatencyFresherThan(long freshnessThreshold) {
		return this.latencyInMillisStatistic.getOldestValue().getTimestamp() >= freshnessThreshold;
	}

	public boolean isOutputBufferLatencyFresherThan(long freshnessThreshold) {
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
}
