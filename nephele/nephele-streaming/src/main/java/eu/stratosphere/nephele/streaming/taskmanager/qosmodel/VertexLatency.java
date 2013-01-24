package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;


public class VertexLatency {

	private ProfilingVertex vertex;

	private ProfilingValueStatistic latencyStatistics;
	
	private final static int DEFAULT_NO_OF_STATISTICS_ENTRIES = 4;
	
	public VertexLatency(ProfilingVertex vertex) {
		this(vertex, DEFAULT_NO_OF_STATISTICS_ENTRIES);
	}

	public VertexLatency(ProfilingVertex vertex, int noOfStatisticsEntries) {
		this.vertex = vertex;
		this.latencyStatistics = new ProfilingValueStatistic(
				noOfStatisticsEntries);
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
