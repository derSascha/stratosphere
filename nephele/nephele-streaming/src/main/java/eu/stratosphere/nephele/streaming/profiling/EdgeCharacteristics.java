package eu.stratosphere.nephele.streaming.profiling;

import eu.stratosphere.nephele.managementgraph.ManagementEdge;

public class EdgeCharacteristics {

	private ManagementEdge edge;

	private ProfilingValueStatistic latencyInMillisStatistic;

	private ProfilingValueStatistic throughputInMbitStatistic;

	private ProfilingValueStatistic outputBufferLifetimeStatistic;

	private boolean isInChain;

	public EdgeCharacteristics(ManagementEdge edge) {
		this.edge = edge;
		this.isInChain = false;
		this.latencyInMillisStatistic = new ProfilingValueStatistic(5);
		this.throughputInMbitStatistic = new ProfilingValueStatistic(5);
		this.outputBufferLifetimeStatistic = new ProfilingValueStatistic(5);
	}

	public ManagementEdge getEdge() {
		return edge;
	}

	public double getChannelLatencyInMillis() {
		if (latencyInMillisStatistic.hasValues()) {
			return latencyInMillisStatistic.getArithmeticMean();
		} else {
			return -1;
		}
	}

	public double getChannelThroughputInMbit() {
		if (throughputInMbitStatistic.hasValues()) {
			return throughputInMbitStatistic.getArithmeticMean();
		} else {
			return -1;
		}
	}

	public double getOutputBufferLifetimeInMillis() {
		if (isInChain) {
			return 0;
		} else {

			if (outputBufferLifetimeStatistic.hasValues()) {
				return outputBufferLifetimeStatistic.getArithmeticMean();
			} else {
				return -1;
			}
		}
	}

	public void addLatencyMeasurement(long timestamp, double latencyInMillis) {
		ProfilingValue value = new ProfilingValue(latencyInMillis, timestamp);
		this.latencyInMillisStatistic.addValue(value);
	}

	public void addThroughputMeasurement(long timestamp, double throughputInMbit) {
		ProfilingValue value = new ProfilingValue(throughputInMbit, timestamp);
		this.throughputInMbitStatistic.addValue(value);
	}

	public void addOutputBufferLatencyMeasurement(long timestamp, double latencyInMillis) {
		ProfilingValue value = new ProfilingValue(latencyInMillis, timestamp);
		this.outputBufferLifetimeStatistic.addValue(value);
	}

	public boolean isChannelLatencyFresherThan(long freshnessThreshold) {
		return latencyInMillisStatistic.getOldestValue().getTimestamp() >= freshnessThreshold;
	}

	public boolean isOutputBufferLatencyFresherThan(long freshnessThreshold) {
		return outputBufferLifetimeStatistic.getOldestValue().getTimestamp() >= freshnessThreshold;
	}

	public void setIsInChain(boolean isInChain) {
		this.isInChain = isInChain;
		if (isInChain) {
			outputBufferLifetimeStatistic = null;
		} else {
			outputBufferLifetimeStatistic = new ProfilingValueStatistic(5);
		}
	}

	public boolean isInChain() {
		return isInChain;
	}

	public boolean isActive() {
		return getChannelLatencyInMillis() != -1 && getOutputBufferLifetimeInMillis() != -1;
	}
}
