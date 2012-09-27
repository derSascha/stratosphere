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
		return vertex;
	}

	public double getLatencyInMillis() {
		if (latencyStatistics.hasValues()) {
			return latencyStatistics.getArithmeticMean();
		} else {
			return -1;
		}
	}
	
	public boolean isActive() {
		return latencyStatistics.hasValues();
	}

	public void addLatencyMeasurement(long timestamp, double latencyInMillis) {
		ProfilingValue value = new ProfilingValue(latencyInMillis, timestamp);
		latencyStatistics.addValue(value);
	}

	@Override
	public String toString() {
		return String.format("VertexLatency[%s|%.03f]", vertex.toString(), getLatencyInMillis());
	}
}
