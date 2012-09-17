package eu.stratosphere.nephele.streaming.listeners;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.streaming.types.profiling.TaskLatency;

/**
 * Handles the measurement and reporting of task latencies.
 * For tasks of type regular, this class will perform latency measurements
 * every {@link #measurementIntervalInRecords} records. A measurement here is the time
 * between the reception of a record and the next emission of a record (the task may
 * consume additional records during the measurement without emitting).
 * For input tasks (they only emit records) the average time between record emissions will be interpreted
 * as the task lateny (this may or may not be correct).
 * For output tasks (they only consume records) the average time between record consumptions will be interpreted
 * as the task lateny (this may or may not be correct, for example if there are no records to consume).
 * Reports will be sent approximately every {@link #reportingIntervalInMillis} millis ("approximately" because if
 * no records are received or emitted at alö
 * 
 * @author Bjoern Lohrmann
 */
public class TaskLatencyReporter {

	//private static final Log LOG = LogFactory.getLog(TaskLatencyReporter.class);

	private ExecutionVertexID vertexId;

	private StreamListenerContext listenerContext;

	private int measurementIntervalInRecords;

	private int recordsSinceLastMeasurement;

	private long timeOfNextReport;

	private boolean measurementInProgress;

	private long lastRecordReceiveTime;

	private long accumulatedLatency;

	private int noOfMeasurements;

	public TaskLatencyReporter(StreamListenerContext listenerContext) {

		this.vertexId = listenerContext.getVertexID();
		this.measurementIntervalInRecords = listenerContext.getTaggingInterval();
		this.listenerContext = listenerContext;

		this.recordsSinceLastMeasurement = 0;
		long now = System.currentTimeMillis();
		this.timeOfNextReport = now + listenerContext.getAggregationInterval();
		this.measurementInProgress = false;
		this.accumulatedLatency = 0;
		this.lastRecordReceiveTime = -1;
		this.accumulatedLatency = 0;
		this.noOfMeasurements = 0;
	}

	public boolean processRecordReceived() {
		boolean reportSent = false;
		recordsSinceLastMeasurement++;

		if (listenerContext.isRegularVertex()) {
			if (isMeasurementDue()) {
				beginRegularTaskLatencyMeasurement();
			}
		} else if (listenerContext.isOutputVertex()) {

			if (isMeasurementDue()) {
				long now = System.currentTimeMillis();
				if (isReportDue(now)) {
					long latency = (now - timeOfNextReport) / recordsSinceLastMeasurement;
					doReport(new TaskLatency(vertexId, latency));
					recordsSinceLastMeasurement = 0;
					reportSent = true;
				}
			}
		} // input vertices never receive records

		return reportSent;
	}

	private void beginRegularTaskLatencyMeasurement() {
		measurementInProgress = true;
		lastRecordReceiveTime = System.currentTimeMillis();
	}

	private long finishRegularTaskLatencyMeasurement(long now) {
		measurementInProgress = false;
		accumulatedLatency += now - lastRecordReceiveTime;
		noOfMeasurements++;
		recordsSinceLastMeasurement = 0;
		return now;
	}

	private void doReport(TaskLatency taskLatency) {
		this.listenerContext.getProfilingReporter().addToNextReport(taskLatency);
		timeOfNextReport = System.currentTimeMillis() + listenerContext.getAggregationInterval();
	}

	private boolean isMeasurementDue() {
		return recordsSinceLastMeasurement >= measurementIntervalInRecords && !measurementInProgress;
	}

	private boolean isReportDue(long now) {
		return now > timeOfNextReport;
	}

	public boolean processRecordEmitted() {
		boolean reportSent = false;

		if (listenerContext.isRegularVertex()) {
			if (measurementInProgress) {
				long now = System.currentTimeMillis();
				finishRegularTaskLatencyMeasurement(now);

				if (isReportDue(now)) {
					long latency = accumulatedLatency / noOfMeasurements;
					doReport(new TaskLatency(vertexId, latency));
					accumulatedLatency = 0;
					noOfMeasurements = 0;
					reportSent = true;
				}
			}
		} else if (listenerContext.isInputVertex()) {
			recordsSinceLastMeasurement++;
			if (isMeasurementDue()) {
				long now = System.currentTimeMillis();
				if (isReportDue(now)) {
					long latency = (now - timeOfNextReport) / recordsSinceLastMeasurement;
					doReport(new TaskLatency(vertexId, latency));
					recordsSinceLastMeasurement = 0;
					reportSent = true;
				}

			}
		} // output vertices never emit records

		return reportSent;
	}
}
