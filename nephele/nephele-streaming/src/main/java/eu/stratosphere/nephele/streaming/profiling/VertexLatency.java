package eu.stratosphere.nephele.streaming.profiling;

import eu.stratosphere.nephele.streaming.profiling.model.ProfilingVertex;

public class VertexLatency {

	private ProfilingVertex vertex;

	private ProfilingValueStatistic latencyStatistics;

	public VertexLatency(ProfilingVertex vertex) {
		this.vertex = vertex;
		this.latencyStatistics = new ProfilingValueStatistic(10);
	}

	public ProfilingVertex getVertex() {
		return this.vertex;
	}

	public double getLatencyInMillis() {
		if (this.latencyStatistics.hasValues()) {
			return this.latencyStatistics.getArithmeticMean();
		}
		return -1;
	}

	public boolean isActive() {
		return this.latencyStatistics.hasValues();
	}

	public void addLatencyMeasurement(long timestamp, double latencyInMillis) {
		ProfilingValue value = new ProfilingValue(latencyInMillis, timestamp);
		this.latencyStatistics.addValue(value);
	}

	@Override
	public String toString() {
		return String.format("VertexLatency[%s|%.03f]", this.vertex.toString(),
				this.getLatencyInMillis());
	}
}
