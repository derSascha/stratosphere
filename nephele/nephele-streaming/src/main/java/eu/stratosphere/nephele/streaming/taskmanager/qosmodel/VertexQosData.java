package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;


public class VertexQosData {

	private QosVertex vertex;

	private QosStatistic latencyStatistics;
	
	private final static int DEFAULT_NO_OF_STATISTICS_ENTRIES = 4;
	
	public VertexQosData(QosVertex vertex) {
		this(vertex, DEFAULT_NO_OF_STATISTICS_ENTRIES);
	}

	public VertexQosData(QosVertex vertex, int noOfStatisticsEntries) {
		this.vertex = vertex;
		this.latencyStatistics = new QosStatistic(
				noOfStatisticsEntries);
	}

	public QosVertex getVertex() {
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
		QosValue value = new QosValue(latencyInMillis, timestamp);
		this.latencyStatistics.addValue(value);
	}

	@Override
	public String toString() {
		return String.format("VertexLatency[%s|%.03f]", this.vertex.toString(),
				this.getLatencyInMillis());
	}
}
