package eu.stratosphere.nephele.streaming.profiling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.StreamingCommunicationThread;
import eu.stratosphere.nephele.streaming.StreamingTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.types.StreamProfilingReport;
import eu.stratosphere.nephele.streaming.types.StreamProfilingReporterInfo;
import eu.stratosphere.nephele.streaming.types.profiling.AbstractStreamProfilingRecord;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.profiling.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.types.profiling.TaskLatency;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * For a given Nephele job, this class aggregates and reports stream profiling data (latencies, throughput, etc) of
 * the tasks of running within the task manager. Profiling data is aggregated for every profiling master
 * and shipped in a single message to the profiling master once every {@link #aggregationInterval}.
 * If no profiling data has been received, messages will be skipped.
 * This class starts its own thread as soon as there is at least on registered task (see
 * {@link #registerProfilingReporterInfo(StreamProfilingReporterInfo)}) and can be shut down by invoking
 * {@link #shutdown()}.
 * This class is threadsafe. Any profiling data added after shutdown will be ignored.
 * FIXME: lifecycle. When does does thread get stopped?
 * 
 * @author Bjoern Lohrmann
 */
public class StreamProfilingReporterThread extends Thread {

	private static final Log LOG = LogFactory.getLog(StreamProfilingReporterThread.class);

	private JobID jobID;

	private StreamingCommunicationThread communicationThread;

	private long aggregationInterval;

	private PendingReport[] pendingReports;

	private Map<InstanceConnectionInfo, PendingReport> reportByProfilingMaster;

	private Map<ExecutionVertexID, Set<PendingReport>> reportByVertexID;

	private Map<ChannelID, Set<PendingReport>> reportByChannelID;

	private LinkedBlockingQueue<AbstractStreamProfilingRecord> pendingProfilingRecords;

	private volatile boolean started;

	private InstanceConnectionInfo localhost;

	private class PendingReport implements Comparable<PendingReport> {
		private long dueTime;

		private StreamProfilingReport report;

		private InstanceConnectionInfo profilingMaster;

		private long reportingOffset;

		public PendingReport(InstanceConnectionInfo profilingMaster, long reportingOffset) {
			this.profilingMaster = profilingMaster;
			this.report = new StreamProfilingReport(jobID);
			long now = System.currentTimeMillis();
			this.dueTime = now - (now % aggregationInterval) + reportingOffset;
		}

		public long getDueTime() {
			return dueTime;
		}

		public StreamProfilingReport getReport() {
			return report;
		}

		public InstanceConnectionInfo getProfilingMaster() {
			return this.profilingMaster;
		}

		public void refreshReport() {
			long now = System.currentTimeMillis();
			while (this.dueTime <= now) {
				this.dueTime = this.dueTime + aggregationInterval;
			}
			report = new StreamProfilingReport(jobID);
		}

		public long getReportingOffset() {
			return this.reportingOffset;
		}

		@Override
		public int compareTo(PendingReport o) {
			if (getReportingOffset() < o.getReportingOffset()) {
				return -1;
			} else if (getReportingOffset() > o.getReportingOffset()) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	public StreamProfilingReporterThread(JobID jobID,
			StreamingCommunicationThread communicationThread,
			int aggregationInterval) {

		this.jobID = jobID;
		this.communicationThread = communicationThread;
		this.aggregationInterval = aggregationInterval;
		this.pendingReports = new PendingReport[0];

		// concurrent maps are necessary here because THIS thread and and another thread registering/unregistering
		// profiling infos may access the maps concurrently
		this.reportByProfilingMaster = new ConcurrentHashMap<InstanceConnectionInfo, PendingReport>();
		this.reportByVertexID = new ConcurrentHashMap<ExecutionVertexID, Set<PendingReport>>();
		this.reportByChannelID = new ConcurrentHashMap<ChannelID, Set<PendingReport>>();

		this.pendingProfilingRecords = new LinkedBlockingQueue<AbstractStreamProfilingRecord>();
		this.started = false;
	}

	public void run() {
		try {
			int currentReportIndex = -1;
			while (!interrupted()) {

				PendingReport currentReport;
				synchronized (pendingReports) {
					currentReportIndex = (currentReportIndex + 1) % pendingReports.length;
					currentReport = pendingReports[currentReportIndex];
				}

				processPendingProfilingData();
				long sleepTime = Math.max(0, currentReport.getDueTime() - System.currentTimeMillis());
				if (sleepTime > 0) {
					sleep(sleepTime);
				}
				processPendingProfilingData();

				if (!currentReport.getReport().isEmpty()) {
					if (currentReport.getProfilingMaster().equals(localhost)) {
						try {
							StreamingTaskManagerPlugin.getInstance().sendData(currentReport.getReport());
						} catch (IOException e) {
							LOG.error(StringUtils.stringifyException(e));
						}
					} else {
						communicationThread.sendToTaskManagerAsynchronously(currentReport.getProfilingMaster(),
							currentReport.getReport());
					}
				}
				currentReport.refreshReport();

			}
		} catch (InterruptedException e) {
		}

		cleanUp();
	}

	private void cleanUp() {
		this.pendingReports = null;
		this.reportByProfilingMaster = null;
		this.reportByVertexID = null;
		this.pendingProfilingRecords = null;
	}

	private ArrayList<AbstractStreamProfilingRecord> tmpRecords = new ArrayList<AbstractStreamProfilingRecord>();

	private void processPendingProfilingData() {
		pendingProfilingRecords.drainTo(tmpRecords);
		for (AbstractStreamProfilingRecord profilingRecord : tmpRecords) {
			if (profilingRecord instanceof ChannelLatency) {
				processChannelLatency((ChannelLatency) profilingRecord);
			} else if (profilingRecord instanceof ChannelThroughput) {
				processChannelThroughput((ChannelThroughput) profilingRecord);
			} else if (profilingRecord instanceof OutputBufferLatency) {
				processOutputBufferLatency((OutputBufferLatency) profilingRecord);
			} else if (profilingRecord instanceof TaskLatency) {
				processTaskLatency((TaskLatency) profilingRecord);
			}
		}
		tmpRecords.clear();
	}

	private void processTaskLatency(TaskLatency taskLatency) {
		Set<PendingReport> reports = reportByVertexID.get(taskLatency.getVertexID());
		if (reports != null) {
			for (PendingReport report : reports) {
				report.getReport().addTaskLatency(taskLatency);
			}
		}
	}

	private void processOutputBufferLatency(OutputBufferLatency obl) {
		Set<PendingReport> reports = reportByChannelID.get(obl.getSourceChannelID());
		if (reports != null) {
			for (PendingReport report : reports) {
				report.getReport().addOutputBufferLatency(obl);
			}
		}
	}

	private void processChannelThroughput(ChannelThroughput channelThroughput) {
		Set<PendingReport> reports = reportByChannelID.get(channelThroughput.getSourceChannelID());
		if (reports != null) {
			for (PendingReport report : reports) {
				report.getReport().addChannelThroughput(channelThroughput);
			}
		}
	}

	private void processChannelLatency(ChannelLatency channelLatency) {
		Set<PendingReport> reports = reportByChannelID.get(channelLatency.getSourceChannelID());
		if (reports != null) {
			for (PendingReport report : reports) {
				report.getReport().addChannelLatency(channelLatency);
			}
		}
	}

	public void registerProfilingReporterInfo(StreamProfilingReporterInfo reporterInfo) {
		synchronized (this.pendingReports) {
			this.localhost = reporterInfo.getReporterConnectionInfo();
			int tasks = 0;
			int channels = 0;

			for (ExecutionVertexID vertexID : reporterInfo.getTasksToProfile()) {
				Set<PendingReport> vertexReports = getOrCreateVertexReports(vertexID);

				for (InstanceConnectionInfo profilingMaster : reporterInfo.getTaskProfilingMasters(vertexID)) {
					vertexReports.add(getOrCreateProfilingReport(profilingMaster));
				}
				LOG.info(String.format("Registered %d reports for vertex %s", vertexReports.size(), vertexID.toString()));
				tasks++;
			}

			for (ChannelID channelID : reporterInfo.getChannelsToProfile()) {
				Set<PendingReport> channelReports = getOrCreateChannelReports(channelID);

				for (InstanceConnectionInfo channelProfilingMaster : reporterInfo
					.getChannelProfilingMasters(channelID)) {

					channelReports.add(getOrCreateProfilingReport(channelProfilingMaster));
				}
				LOG.info(String.format("Registered %d reports for channel %s", channelReports.size(),
					channelID.toString()));
				channels++;
			}
			LOG.info(String.format("Added %d tasks and %d channels. Altogether (max) %d reports each interval", tasks,
				channels, reportByProfilingMaster.size()));

			if (!started) {
				start();
				started = true;
			}
		}
	}

	/**
	 * @param reporterInfo
	 * @return true when the reporter is shutting down, false otherwise
	 */
	public boolean unregisterProfilingReporterInfo(StreamProfilingReporterInfo reporterInfo) {
		// no synchronization necessary here, because reportByXXX maps are threadsafe
		for (ExecutionVertexID vertexID : reporterInfo.getTasksToProfile()) {
			this.reportByVertexID.remove(vertexID);
		}

		for (ChannelID channelID : reporterInfo.getChannelsToProfile()) {
			this.reportByChannelID.remove(channelID);
		}

		// TODO: reduce the pending reports array accordingly (currently we don't have
		// the lookup structures to now whether a PendingReport won't be needed anymore)
		if (this.reportByVertexID.isEmpty() && this.reportByChannelID.isEmpty()) {
			shutdown();
			return true;
		} else {
			return false;
		}
	}

	private Set<PendingReport> getOrCreateChannelReports(ChannelID channelID) {
		Set<PendingReport> channelReports = reportByChannelID.get(channelID);
		if (channelReports == null) {
			// concurrent sets are necessary here because THIS thread and and another thread registering/unregistering
			// profiling infos may access the maps concurrently
			channelReports = new CopyOnWriteArraySet<PendingReport>();
			reportByChannelID.put(channelID, channelReports);
		}
		return channelReports;
	}

	private Set<PendingReport> getOrCreateVertexReports(ExecutionVertexID vertexID) {
		Set<PendingReport> vertexReports = reportByVertexID.get(vertexID);
		if (vertexReports == null) {
			// concurrent sets are necessary here because THIS thread and and another thread registering/unregistering
			// profiling infos may access the maps concurrently
			vertexReports = new CopyOnWriteArraySet<PendingReport>();
			reportByVertexID.put(vertexID, vertexReports);
		}
		return vertexReports;
	}

	private PendingReport getOrCreateProfilingReport(InstanceConnectionInfo profilingMaster) {
		PendingReport profilingMasterReport = reportByProfilingMaster.get(profilingMaster);
		if (profilingMasterReport == null) {
			profilingMasterReport = createAndEnqueueReport(profilingMaster);
		}
		return profilingMasterReport;
	}

	private PendingReport createAndEnqueueReport(InstanceConnectionInfo profilingMaster) {
		PendingReport newReport = new PendingReport(profilingMaster, (long) (Math.random() * aggregationInterval));
		reportByProfilingMaster.put(profilingMaster, newReport);

		// insert new report in pendingReports, sorted by reporting offset
		PendingReport[] newPendingReports = new PendingReport[pendingReports.length + 1];
		System.arraycopy(this.pendingReports, 0, newPendingReports, 0, this.pendingReports.length);
		newPendingReports[newPendingReports.length - 1] = newReport;
		Arrays.sort(newPendingReports);
		this.pendingReports = newPendingReports;

		return newReport;
	}

	public void addToNextReport(ChannelLatency channelLatency) {
		// may be null when profiling was shut down
		if (pendingProfilingRecords != null) {
			pendingProfilingRecords.add(channelLatency);
		}
	}

	public void addToNextReport(ChannelThroughput channelThroughput) {
		// may be null when profiling was shut down
		if (pendingProfilingRecords != null) {
			pendingProfilingRecords.add(channelThroughput);
		}
	}

	public void addToNextReport(OutputBufferLatency outputBufferLatency) {
		// may be null when profiling was shut down
		if (pendingProfilingRecords != null) {
			pendingProfilingRecords.add(outputBufferLatency);
		}
	}

	public void addToNextReport(TaskLatency taskLatency) {
		// may be null when profiling was shut down
		if (pendingProfilingRecords != null) {
			pendingProfilingRecords.add(taskLatency);
		}
	}

	public void shutdown() {
		interrupt();
	}
}
