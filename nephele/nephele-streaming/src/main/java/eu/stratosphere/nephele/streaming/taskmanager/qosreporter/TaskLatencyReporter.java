package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.streaming.message.profiling.TaskLatency;

/**
 * Handles the measurement and reporting of task latencies. For tasks of type
 * regular, this class will perform latency measurements every
 * {@link #measurementIntervalInRecords} records. A measurement here is the time
 * between the reception of a record and the next emission of a record (the task
 * may consume additional records during the measurement without emitting). For
 * input tasks (they only emit records) the average time between record
 * emissions will be interpreted as the task lateny (this may or may not be
 * correct). For output tasks (they only consume records) the average time
 * between record consumptions will be interpreted as the task lateny (this may
 * or may not be correct, for example if there are no records to consume).
 * Reports will be sent approximately every {@link #reportingIntervalInMillis}
 * millis ("approximately" because if no records are received or emitted at alö
 * 
 * @author Bjoern Lohrmann
 */
public class TaskLatencyReporter {

	// private static final Log LOG =
	// LogFactory.getLog(TaskLatencyReporter.class);

	private ExecutionVertexID vertexId;

	private int recordsSinceLastMeasurement;

	private long timeOfNextReport;

	private boolean measurementInProgress;

	private long lastRecordReceiveTime;

	private long accumulatedLatency;

	private int noOfMeasurements;

	private QosReporterThread qosReporter;

	private TaskType taskType;

	public static enum TaskType {
		INPUT, REGULAR, OUTPUT
	};

	public TaskLatencyReporter(ExecutionVertexID vertexID, TaskType taskType,
			QosReporterThread qosReporter) {

		this.vertexId = vertexID;
		this.taskType = taskType;
		this.qosReporter = qosReporter;

		this.recordsSinceLastMeasurement = 0;
		long now = System.currentTimeMillis();
		this.timeOfNextReport = now
				+ qosReporter.getConfiguration().getAggregationInterval();
		this.measurementInProgress = false;
		this.accumulatedLatency = 0;
		this.lastRecordReceiveTime = -1;
		this.accumulatedLatency = 0;
		this.noOfMeasurements = 0;
	}

	public boolean processRecordReceived() {
		boolean reportSent = false;
		this.recordsSinceLastMeasurement++;

		if (this.isMeasurementDue()) {
			switch (this.taskType) {
			case REGULAR:
				this.beginRegularTaskLatencyMeasurement();
				break;
			case OUTPUT:
				long now = System.currentTimeMillis();
				if (this.isReportDue(now)) {
					long latency = (now - this.timeOfNextReport)
							/ this.recordsSinceLastMeasurement;
					this.doReport(new TaskLatency(this.vertexId, latency));
					this.recordsSinceLastMeasurement = 0;
					reportSent = true;
				}
				break;
			case INPUT:
				// input vertices never receive records
				break;
			}
		}

		return reportSent;
	}

	private void beginRegularTaskLatencyMeasurement() {
		this.measurementInProgress = true;
		this.lastRecordReceiveTime = System.currentTimeMillis();
	}

	private long finishRegularTaskLatencyMeasurement(long now) {
		this.measurementInProgress = false;
		this.accumulatedLatency += now - this.lastRecordReceiveTime;
		this.noOfMeasurements++;
		this.recordsSinceLastMeasurement = 0;
		return now;
	}

	private void doReport(TaskLatency taskLatency) {
		this.qosReporter.addToNextReport(taskLatency);
		this.timeOfNextReport = System.currentTimeMillis()
				+ this.qosReporter.getConfiguration().getAggregationInterval();
	}

	private boolean isMeasurementDue() {
		return this.recordsSinceLastMeasurement >= this.qosReporter
				.getConfiguration().getTaggingInterval()
				&& !this.measurementInProgress;
	}

	private boolean isReportDue(long now) {
		return now > this.timeOfNextReport;
	}

	public boolean processRecordEmitted() {
		boolean reportSent = false;

		switch (this.taskType) {
		case REGULAR:
			if (this.measurementInProgress) {
				long now = System.currentTimeMillis();
				this.finishRegularTaskLatencyMeasurement(now);

				if (this.isReportDue(now)) {
					long latency = this.accumulatedLatency
							/ this.noOfMeasurements;
					this.doReport(new TaskLatency(this.vertexId, latency));
					this.accumulatedLatency = 0;
					this.noOfMeasurements = 0;
					reportSent = true;
				}
			}
			break;
		case INPUT:
			this.recordsSinceLastMeasurement++;
			if (this.isMeasurementDue()) {
				long now = System.currentTimeMillis();
				if (this.isReportDue(now)) {
					long latency = (now - this.timeOfNextReport)
							/ this.recordsSinceLastMeasurement;
					this.doReport(new TaskLatency(this.vertexId, latency));
					this.recordsSinceLastMeasurement = 0;
					reportSent = true;
				}

			}
			break;
		case OUTPUT:
			// output vertices never emit records
			break;
		}

		return reportSent;
	}
}
