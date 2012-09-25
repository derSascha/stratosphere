package eu.stratosphere.nephele.streaming.profiling;

import java.util.HashSet;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.StreamingCommunicationThread;
import eu.stratosphere.nephele.streaming.types.StreamProfilingReport;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.profiling.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.types.profiling.TaskLatency;

/**
 * For a given Nephele job, this class aggregates stream profiling data (latencies, throughput, etc) of
 * the tasks of running within each task manager and ships the aggregated data once every {@link #aggregationInterval}
 * to the job manager in a single message. If no profiling data has been received, the message will be skipped.
 * 
 * This class starts its own thread as soon as there is at least on registered task (see
 * {@link #registerTask(ExecutionVertexID)}) and stops the thread as soon as the last task
 * is unregistered (see {@link #unregisterTask(ExecutionVertexID)}).
 * 
 * This class is threadsafe.
 * 
 * @author Bjoern Lohrmann
 */
public class JobStreamProfilingReporter extends Thread {

	private JobID jobID;

	private StreamingCommunicationThread communicationThread;

	private long aggregationInterval;

	private long nextReportDue;

	private StreamProfilingReport nextReport;

	private HashSet<ExecutionVertexID> registeredTasksForJob;

	public JobStreamProfilingReporter(JobID jobID,
			StreamingCommunicationThread communicationThread,
			int aggregationInterval) {

		this.jobID = jobID;
		this.communicationThread = communicationThread;
		this.aggregationInterval = aggregationInterval;
		long reportingOffset = (long) (Math.random() * aggregationInterval);
		long now = System.currentTimeMillis();
		this.nextReportDue = now - (now % aggregationInterval) + reportingOffset;

		this.nextReport = new StreamProfilingReport(jobID);
		this.registeredTasksForJob = new HashSet<ExecutionVertexID>();
	}

	public void run() {
		try {
			while (!interrupted()) {
				long now = System.currentTimeMillis();
				setNextReportDue(now);
				long sleepTime = Math.max(0, nextReportDue - now);
				sleep(sleepTime);

				synchronized (this) {
					if (!nextReport.isEmpty()) {
						communicationThread.sendDataAsynchronously(nextReport);
						nextReport = new StreamProfilingReport(jobID);
					}
					setNextReportDue(System.currentTimeMillis());
				}
			}
		} catch (InterruptedException e) {
		}
	}

	public synchronized void registerTask(ExecutionVertexID vertexID) {
		registeredTasksForJob.add(vertexID);
		if (!isAlive()) {
			start();
		}
	}

	public synchronized void unregisterTask(ExecutionVertexID vertexID) {
		registeredTasksForJob.remove(vertexID);
		if (registeredTasksForJob.isEmpty()) {
			interrupt();
		}
	}

	public synchronized boolean hasRegisteredTasks() {
		return !registeredTasksForJob.isEmpty();
	}

	private void setNextReportDue(long now) {
		while (this.nextReportDue <= now) {
			this.nextReportDue = this.nextReportDue + aggregationInterval;
		}
	}

	public synchronized void addToNextReport(ChannelLatency channelLatency) {
		nextReport.addChannelLatency(channelLatency);
	}

	public synchronized void addToNextReport(ChannelThroughput channelThroughput) {
		nextReport.addChannelThroughput(channelThroughput);
	}

	public synchronized void addToNextReport(OutputBufferLatency outputBufferLatency) {
		nextReport.addOutputBufferLatency(outputBufferLatency);
	}

	public synchronized void addToNextReport(TaskLatency taskLatency) {
		nextReport.addTaskLatency(taskLatency);
	}
}
