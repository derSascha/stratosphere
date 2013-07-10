package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

public class VertexQosData {

	private QosVertex vertex;

	/**
	 * Indexed by (inputGateIndex, outputGateIndex) of the vertex.
	 */
	private QosStatistic[][] qosStatistics;

	private final static int DEFAULT_NO_OF_STATISTICS_ENTRIES = 4;

	public VertexQosData(QosVertex vertex) {
		this.vertex = vertex;
		this.qosStatistics = new QosStatistic[1][1];
	}

	public QosVertex getVertex() {
		return this.vertex;
	}

	public double getLatencyInMillis(int inputGateIndex, int outputGateIndex) {
		QosStatistic statistic = this.qosStatistics[inputGateIndex][outputGateIndex];
		
		if (statistic.hasValues()) {
			return statistic.getArithmeticMean();
		}
		return -1;
	}

	public boolean isActive(int inputGateIndex, int outputGateIndex) {
		return this.qosStatistics[inputGateIndex][outputGateIndex].hasValues();
	}

	public void prepareForReporsOnGateCombination(int inputGateIndex,
			int outputGateIndex) {

		if (this.qosStatistics.length <= inputGateIndex) {
			QosStatistic[][] newArray = new QosStatistic[inputGateIndex + 1][];
			System.arraycopy(this.qosStatistics, 0, newArray, 0,
					this.qosStatistics.length);
		}

		if (this.qosStatistics[inputGateIndex] == null) {
			this.qosStatistics[inputGateIndex] = new QosStatistic[outputGateIndex + 1];
		}

		if (this.qosStatistics[inputGateIndex].length <= outputGateIndex) {
			QosStatistic[] newArray = new QosStatistic[outputGateIndex + 1];
			System.arraycopy(this.qosStatistics[inputGateIndex], 0, newArray,
					0, this.qosStatistics[inputGateIndex].length);
		}

		if (this.qosStatistics[inputGateIndex][outputGateIndex] == null) {
			this.qosStatistics[inputGateIndex][outputGateIndex] = new QosStatistic(
					DEFAULT_NO_OF_STATISTICS_ENTRIES);
		}
	}

	public void addLatencyMeasurement(int inputGateIndex, int outputGateIndex,
			long timestamp, double latencyInMillis) {

		QosValue value = new QosValue(latencyInMillis, timestamp);
		this.qosStatistics[inputGateIndex][outputGateIndex].addValue(value);
	}
}
