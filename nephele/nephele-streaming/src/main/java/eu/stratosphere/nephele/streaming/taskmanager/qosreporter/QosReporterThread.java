package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import eu.stratosphere.nephele.streaming.message.action.ActAsQosReporterAction;
import eu.stratosphere.nephele.streaming.message.profiling.AbstractStreamProfilingRecord;
import eu.stratosphere.nephele.streaming.message.profiling.ChannelLatency;
import eu.stratosphere.nephele.streaming.message.profiling.ChannelThroughput;
import eu.stratosphere.nephele.streaming.message.profiling.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.message.profiling.StreamProfilingReport;
import eu.stratosphere.nephele.streaming.message.profiling.TaskLatency;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.StreamTaskManagerPlugin;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * For a given Nephele job, this class aggregates and reports stream profiling
 * data (latencies, throughput, etc) of the tasks of running within the task
 * manager. Profiling data is aggregated for every profiling master and shipped
 * in a single message to the profiling master once every
 * {@link #aggregationInterval}. If no profiling data has been received,
 * messages will be skipped. This class starts its own thread as soon as there
 * is at least on registered task (see
 * {@link #registerQosReporter(ActAsQosReporterAction)}) and can be shut down by
 * invoking {@link #shutdown()}. This class is threadsafe. Any profiling data
 * added after shutdown will be ignored.
 * 
 * FIXME: lifecycle. When does does thread
 * get stopped?
 * 
 * @author Bjoern Lohrmann
 */
public class QosReporterThread extends Thread {

	private static final Log LOG = LogFactory.getLog(QosReporterThread.class);

	private final JobID jobID;

	private final StreamMessagingThread messagingThread;

	private final QosReporterConfiguration reporterConfig;

	private PendingReport[] pendingReports;

	private int currentReportIndex;

	private final ConcurrentHashMap<InstanceConnectionInfo, PendingReport> reportByQosManager;

	private final ConcurrentHashMap<ExecutionVertexID, Set<PendingReport>> reportByVertexID;

	private final ConcurrentHashMap<ChannelID, Set<PendingReport>> reportByChannelID;

	private final LinkedBlockingQueue<AbstractStreamProfilingRecord> pendingProfilingRecords;

	private volatile boolean started;

	private InstanceConnectionInfo localhost;

	private class PendingReport implements Comparable<PendingReport> {
		private long dueTime;

		private StreamProfilingReport report;

		private InstanceConnectionInfo qosManager;

		private long reportingOffset;

		public PendingReport(InstanceConnectionInfo qosManager,
				long reportingOffset) {
			this.qosManager = qosManager;
			this.report = new StreamProfilingReport(
					QosReporterThread.this.jobID);
			long now = System.currentTimeMillis();
			this.dueTime = now
					- now
					% QosReporterThread.this.reporterConfig
							.getAggregationInterval() + reportingOffset;
		}

		public long getDueTime() {
			return this.dueTime;
		}

		public StreamProfilingReport getReport() {
			return this.report;
		}

		public InstanceConnectionInfo getQosManager() {
			return this.qosManager;
		}

		public void refreshReport() {
			long now = System.currentTimeMillis();
			while (this.dueTime <= now) {
				this.dueTime = this.dueTime
						+ QosReporterThread.this.reporterConfig
								.getAggregationInterval();
			}
			this.report = new StreamProfilingReport(
					QosReporterThread.this.jobID);
		}

		public long getReportingOffset() {
			return this.reportingOffset;
		}

		public boolean isEmpty() {
			return this.report.isEmpty();
		}

		@Override
		public int compareTo(PendingReport o) {
			if (this.getReportingOffset() < o.getReportingOffset()) {
				return -1;
			} else if (this.getReportingOffset() > o.getReportingOffset()) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	public QosReporterThread(JobID jobID,
			StreamMessagingThread messagingThread,
			QosReporterConfiguration reporterConfig) {

		this.jobID = jobID;
		this.messagingThread = messagingThread;
		this.reporterConfig = reporterConfig;
		this.pendingReports = new PendingReport[0];
		this.currentReportIndex = -1;

		// concurrent maps are necessary here because THIS thread and and
		// another thread registering/unregistering
		// profiling infos may access the maps concurrently
		this.reportByQosManager = new ConcurrentHashMap<InstanceConnectionInfo, PendingReport>();
		this.reportByVertexID = new ConcurrentHashMap<ExecutionVertexID, Set<PendingReport>>();
		this.reportByChannelID = new ConcurrentHashMap<ChannelID, Set<PendingReport>>();

		this.pendingProfilingRecords = new LinkedBlockingQueue<AbstractStreamProfilingRecord>();
		this.started = false;
	}

	@Override
	public void run() {
		try {
			while (!interrupted()) {

				PendingReport currentReport = getCurrentReport();
				this.processPendingProfilingData();
				this.sleepUntilReportDue(currentReport);
				this.processPendingProfilingData();

				if (!currentReport.isEmpty()) {
					if (isLocalReport(currentReport)) {
						sendToLocal(currentReport);
					} else {
						sendToRemote(currentReport);
					}
				}
				currentReport.refreshReport();
			}
		} catch (InterruptedException e) {
		}

		this.cleanUp();
	}

	private boolean isLocalReport(PendingReport currentReport) {
		return currentReport.getQosManager().equals(this.localhost);
	}

	private void sendToRemote(PendingReport currentReport)
			throws InterruptedException {
		this.messagingThread.sendToTaskManagerAsynchronously(
				currentReport.getQosManager(), currentReport.getReport());
	}

	/**
	 * @param currentReport
	 */
	private void sendToLocal(PendingReport currentReport) {
		try {
			StreamTaskManagerPlugin.getInstance().sendData(
					currentReport.getReport());
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}

	/**
	 * @param currentReport
	 * @throws InterruptedException
	 */
	private void sleepUntilReportDue(PendingReport currentReport)
			throws InterruptedException {
		long sleepTime = Math.max(0,
				currentReport.getDueTime() - System.currentTimeMillis());
		if (sleepTime > 0) {
			sleep(sleepTime);
		}
	}

	/**
	 * @return
	 */
	private PendingReport getCurrentReport() {
		PendingReport currentReport;
		synchronized (this.pendingReports) {
			this.currentReportIndex = (this.currentReportIndex + 1)
					% this.pendingReports.length;
			currentReport = this.pendingReports[this.currentReportIndex];
		}
		return currentReport;
	}

	private void cleanUp() {
		this.pendingReports = null;
		this.reportByQosManager.clear();
		this.reportByVertexID.clear();
		this.pendingProfilingRecords.clear();
	}

	private ArrayList<AbstractStreamProfilingRecord> tmpRecords = new ArrayList<AbstractStreamProfilingRecord>();

	private void processPendingProfilingData() {
		this.pendingProfilingRecords.drainTo(this.tmpRecords);
		for (AbstractStreamProfilingRecord profilingRecord : this.tmpRecords) {
			if (profilingRecord instanceof ChannelLatency) {
				this.processChannelLatency((ChannelLatency) profilingRecord);
			} else if (profilingRecord instanceof ChannelThroughput) {
				this.processChannelThroughput((ChannelThroughput) profilingRecord);
			} else if (profilingRecord instanceof OutputBufferLatency) {
				this.processOutputBufferLatency((OutputBufferLatency) profilingRecord);
			} else if (profilingRecord instanceof TaskLatency) {
				this.processTaskLatency((TaskLatency) profilingRecord);
			}
		}
		this.tmpRecords.clear();
	}

	private void processTaskLatency(TaskLatency taskLatency) {
		Set<PendingReport> reports = this.reportByVertexID.get(taskLatency
				.getVertexID());
		if (reports != null) {
			for (PendingReport report : reports) {
				report.getReport().addTaskLatency(taskLatency);
			}
		}
	}

	private void processOutputBufferLatency(OutputBufferLatency obl) {
		Set<PendingReport> reports = this.reportByChannelID.get(obl
				.getSourceChannelID());
		if (reports != null) {
			for (PendingReport report : reports) {
				report.getReport().addOutputBufferLatency(obl);
			}
		}
	}

	private void processChannelThroughput(ChannelThroughput channelThroughput) {
		Set<PendingReport> reports = this.reportByChannelID
				.get(channelThroughput.getSourceChannelID());
		if (reports != null) {
			for (PendingReport report : reports) {
				report.getReport().addChannelThroughput(channelThroughput);
			}
		}
	}

	private void processChannelLatency(ChannelLatency channelLatency) {
		Set<PendingReport> reports = this.reportByChannelID.get(channelLatency
				.getSourceChannelID());
		if (reports != null) {
			for (PendingReport report : reports) {
				report.getReport().addChannelLatency(channelLatency);
			}
		}
	}

	public void registerQosReporter(ActAsQosReporterAction reporterAction) {

		synchronized (this.pendingReports) {
			this.localhost = reporterAction.getReporterConnectionInfo();

			registerTasksToProfile(reporterAction);
			registerChannelsToProfile(reporterAction);

			LOG.info(String
					.format("Added %d tasks and %d channels. Max %d reports each interval.",
							reporterAction.getTasksToProfile().size(),
							reporterAction.getChannelsToProfile().size(),
							this.reportByQosManager.size()));

			if (!this.started) {
				this.start();
				this.started = true;
			}
		}
	}

	/**
	 * @param reporterAction
	 */
	private void registerChannelsToProfile(ActAsQosReporterAction reporterAction) {
		for (ChannelID channelID : reporterAction.getChannelsToProfile()) {
			Set<PendingReport> channelReports = this
					.getOrCreateChannelReports(channelID);

			for (InstanceConnectionInfo channelQosManager : reporterAction
					.getChannelQosManagers(channelID)) {

				channelReports.add(this
						.getOrCreateProfilingReport(channelQosManager));
			}
			LOG.info(String.format("Registered %d reports for channel %s",
					channelReports.size(), channelID.toString()));
		}
	}

	/**
	 * @param reporterAction
	 */
	private void registerTasksToProfile(ActAsQosReporterAction reporterAction) {
		for (ExecutionVertexID vertexID : reporterAction.getTasksToProfile()) {
			Set<PendingReport> vertexReports = this
					.getOrCreateVertexReports(vertexID);

			for (InstanceConnectionInfo qosManager : reporterAction
					.getTaskQosManagers(vertexID)) {
				vertexReports.add(this.getOrCreateProfilingReport(qosManager));
			}
			LOG.info(String.format("Registered %d reports for vertex %s",
					vertexReports.size(), vertexID.toString()));
		}
	}

	/**
	 * @param reporterInfo
	 * @return true when the reporter is shutting down, false otherwise
	 */
	public boolean unregisterQosReporter(ActAsQosReporterAction reporterAction) {
		// no synchronization necessary here, because reportByXXX maps are
		// threadsafe
		for (ExecutionVertexID vertexID : reporterAction.getTasksToProfile()) {
			this.reportByVertexID.remove(vertexID);
		}

		for (ChannelID channelID : reporterAction.getChannelsToProfile()) {
			this.reportByChannelID.remove(channelID);
		}

		// TODO: reduce the pending reports array accordingly (currently we
		// don't have
		// the lookup structures to now whether a PendingReport won't be needed
		// anymore)
		if (this.reportByVertexID.isEmpty() && this.reportByChannelID.isEmpty()) {
			this.shutdown();
			return true;
		}
		return false;
	}

	private Set<PendingReport> getOrCreateChannelReports(ChannelID channelID) {
		Set<PendingReport> channelReports = this.reportByChannelID
				.get(channelID);
		if (channelReports == null) {
			// concurrent sets are necessary here because THIS thread and and
			// another thread registering/unregistering
			// profiling infos may access the maps concurrently
			channelReports = new CopyOnWriteArraySet<PendingReport>();
			this.reportByChannelID.put(channelID, channelReports);
		}
		return channelReports;
	}

	private Set<PendingReport> getOrCreateVertexReports(
			ExecutionVertexID vertexID) {
		Set<PendingReport> vertexReports = this.reportByVertexID.get(vertexID);
		if (vertexReports == null) {
			// concurrent sets are necessary here because THIS thread and and
			// another thread registering/unregistering
			// profiling infos may access the maps concurrently
			vertexReports = new CopyOnWriteArraySet<PendingReport>();
			this.reportByVertexID.put(vertexID, vertexReports);
		}
		return vertexReports;
	}

	private PendingReport getOrCreateProfilingReport(
			InstanceConnectionInfo qosManager) {
		PendingReport qosManagerReport = this.reportByQosManager
				.get(qosManager);
		if (qosManagerReport == null) {
			qosManagerReport = this.createAndEnqueueReport(qosManager);
		}
		return qosManagerReport;
	}

	private PendingReport createAndEnqueueReport(
			InstanceConnectionInfo qosManager) {

		PendingReport newReport = new PendingReport(qosManager,
				(long) (Math.random() * this.reporterConfig
						.getAggregationInterval()));
		this.reportByQosManager.put(qosManager, newReport);

		// insert new report in pendingReports, sorted by reporting offset
		PendingReport[] newPendingReports = new PendingReport[this.pendingReports.length + 1];
		System.arraycopy(this.pendingReports, 0, newPendingReports, 0,
				this.pendingReports.length);
		newPendingReports[newPendingReports.length - 1] = newReport;
		Arrays.sort(newPendingReports);
		this.pendingReports = newPendingReports;

		return newReport;
	}
	
	public boolean needsQosReporting(ExecutionVertexID vertexID) {
		return this.reportByVertexID.containsKey(vertexID);
	}

	public boolean needsQosReporting(ChannelID vertexID) {
		return this.reportByChannelID.containsKey(vertexID);
	}

	public void addToNextReport(AbstractStreamProfilingRecord profilingRecord) {
		this.pendingProfilingRecords.add(profilingRecord);
	}

	public void shutdown() {
		this.interrupt();
	}

	public QosReporterConfiguration getConfiguration() {
		return this.reporterConfig;
	}
}
