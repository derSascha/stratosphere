package eu.stratosphere.nephele.streaming.message.profiling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.AbstractStreamMessage;
import eu.stratosphere.nephele.streaming.message.action.EdgeQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.action.VertexQosReporterConfig;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

/**
 * Holds a profiling data report meant to be shipped to a
 * {@link eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosManagerThread}
 * . Instead of sending each {@link AbstractStreamProfilingRecord} individually,
 * they are sent in batch. Most internal fields of this class are initialized in
 * a lazy fashion, thus (empty) instances of this class have a small memory
 * footprint.
 * 
 * @author Bjoern Lohrmann
 */
public class QosReport extends AbstractStreamMessage {

	private HashMap<QosReporterID.Edge, ChannelLatency> channelLatencies;

	private HashMap<QosReporterID.Edge, OutputChannelStatistics> outputChannelStats;

	private HashMap<QosReporterID.Vertex, TaskLatency> taskLatencies;

	private LinkedList<VertexQosReporterConfig> vertexReporterAnnouncements;

	private LinkedList<EdgeQosReporterConfig> edgeReporterAnnouncements;

	/**
	 * Creates and initializes StreamProfilingReport object to be used for
	 * sending/serialization.
	 * 
	 * @param jobID
	 */
	public QosReport(JobID jobID) {
		super(jobID);
	}

	/**
	 * Creates and initializes StreamProfilingReport object to be used for
	 * receiving/deserialization.
	 */
	public QosReport() {
		super();
	}

	private HashMap<QosReporterID.Edge, ChannelLatency> getOrCreateChannelLatencyMap() {
		if (this.channelLatencies == null) {
			this.channelLatencies = new HashMap<QosReporterID.Edge, ChannelLatency>();
		}
		return this.channelLatencies;
	}

	private HashMap<QosReporterID.Edge, OutputChannelStatistics> getOrCreateOutputChannelStatsMap() {
		if (this.outputChannelStats == null) {
			this.outputChannelStats = new HashMap<QosReporterID.Edge, OutputChannelStatistics>();
		}
		return this.outputChannelStats;
	}

	private HashMap<QosReporterID.Vertex, TaskLatency> getOrCreateTaskLatencyMap() {
		if (this.taskLatencies == null) {
			this.taskLatencies = new HashMap<QosReporterID.Vertex, TaskLatency>();
		}
		return this.taskLatencies;
	}

	public void addChannelLatency(ChannelLatency channelLatency) {
		QosReporterID.Edge reporterID = channelLatency.getReporterID();

		ChannelLatency existing = this.getOrCreateChannelLatencyMap().get(
				reporterID);
		if (existing == null) {
			this.getOrCreateChannelLatencyMap().put(reporterID, channelLatency);
		} else {
			existing.add(channelLatency);
		}
	}

	public void announceVertexQosReporter(VertexQosReporterConfig vertexReporter) {
		if (this.vertexReporterAnnouncements == null) {
			this.vertexReporterAnnouncements = new LinkedList<VertexQosReporterConfig>();
		}
		this.vertexReporterAnnouncements.add(vertexReporter);
	}

	public List<VertexQosReporterConfig> getVertexQosReporterAnnouncements() {
		if (this.vertexReporterAnnouncements == null) {
			return Collections.emptyList();
		}
		return this.vertexReporterAnnouncements;
	}

	public void announceEdgeQosReporter(EdgeQosReporterConfig edgeReporter) {
		if (this.edgeReporterAnnouncements == null) {
			this.edgeReporterAnnouncements = new LinkedList<EdgeQosReporterConfig>();
		}
		this.edgeReporterAnnouncements.add(edgeReporter);
	}

	public List<EdgeQosReporterConfig> getEdgeQosReporterAnnouncements() {
		if (this.edgeReporterAnnouncements == null) {
			return Collections.emptyList();
		}
		return this.edgeReporterAnnouncements;
	}

	public Collection<ChannelLatency> getChannelLatencies() {
		if (this.channelLatencies == null) {
			return Collections.emptyList();
		}
		return this.channelLatencies.values();
	}

	public void addOutputChannelStatistics(OutputChannelStatistics channelStats) {

		QosReporterID.Edge reporterID = channelStats.getReporterID();

		OutputChannelStatistics existing = this
				.getOrCreateOutputChannelStatsMap().get(channelStats);
		if (existing == null) {
			this.getOrCreateOutputChannelStatsMap().put(reporterID,
					channelStats);
		} else {
			existing.add(channelStats);
		}
	}

	public Collection<OutputChannelStatistics> getOutputChannelStatistics() {
		if (this.outputChannelStats == null) {
			return Collections.emptyList();
		}
		return this.outputChannelStats.values();
	}

	public void addTaskLatency(TaskLatency taskLatency) {
		QosReporterID.Vertex reporterID = taskLatency.getReporterID();
		TaskLatency existing = this.getOrCreateTaskLatencyMap().get(reporterID);
		if (existing == null) {
			this.getOrCreateTaskLatencyMap().put(reporterID, taskLatency);
		} else {
			existing.add(taskLatency);
		}
	}

	public Collection<TaskLatency> getTaskLatencies() {
		if (this.taskLatencies == null) {
			return Collections.emptyList();
		}
		return this.taskLatencies.values();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		this.writeChannelLatencies(out);
		this.writeOutputChannelStatistics(out);
		this.writeTaskLatencies(out);
		this.writeVertexReporterAnnouncements(out);
		this.writeEdgeReporterAnnouncements(out);
	}

	private void writeEdgeReporterAnnouncements(DataOutput out)
			throws IOException {
		if (this.edgeReporterAnnouncements != null) {
			out.writeInt(this.edgeReporterAnnouncements.size());
			for (EdgeQosReporterConfig reporterConfig : this.edgeReporterAnnouncements) {
				reporterConfig.write(out);
			}
		}
	}

	private void writeVertexReporterAnnouncements(DataOutput out)
			throws IOException {
		if (this.vertexReporterAnnouncements != null) {
			out.writeInt(this.vertexReporterAnnouncements.size());
			for (VertexQosReporterConfig reporterConfig : this.vertexReporterAnnouncements) {
				reporterConfig.write(out);
			}
		}
	}

	private void writeChannelLatencies(DataOutput out) throws IOException {
		if (this.channelLatencies != null) {
			out.writeInt(this.channelLatencies.size());
			for (Entry<QosReporterID.Edge, ChannelLatency> entry : this.channelLatencies
					.entrySet()) {
				entry.getKey().write(out);
				out.writeDouble(entry.getValue().getChannelLatency());
			}
		} else {
			out.writeInt(0);
		}
	}

	private void writeOutputChannelStatistics(DataOutput out)
			throws IOException {
		if (this.outputChannelStats != null) {
			out.writeInt(this.outputChannelStats.size());
			for (Entry<QosReporterID.Edge, OutputChannelStatistics> entry : this.outputChannelStats
					.entrySet()) {
				entry.getKey().write(out);
				out.writeDouble(entry.getValue().getThroughput());
				out.writeDouble(entry.getValue().getOutputBufferLifetime());
				out.writeDouble(entry.getValue().getRecordsPerBuffer());
				out.writeDouble(entry.getValue().getRecordsPerSecond());
			}
		} else {
			out.writeInt(0);
		}
	}

	private void writeTaskLatencies(DataOutput out) throws IOException {
		if (this.taskLatencies != null) {
			out.writeInt(this.taskLatencies.size());
			for (Entry<QosReporterID.Vertex, TaskLatency> entry : this.taskLatencies
					.entrySet()) {
				entry.getKey().write(out);
				out.writeDouble(entry.getValue().getTaskLatency());
			}
		} else {
			out.writeInt(0);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		this.readChannelLatencies(in);
		this.readOutputChannelStatistics(in);
		this.readTaskLatencies(in);
		this.readVertexReporterAnnouncements(in);
		this.readEdgeReporterAnnouncements(in);
	}

	private void readVertexReporterAnnouncements(DataInput in)
			throws IOException {
		this.vertexReporterAnnouncements = new LinkedList<VertexQosReporterConfig>();
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			VertexQosReporterConfig reporterConfig = new VertexQosReporterConfig();
			reporterConfig.read(in);
			this.vertexReporterAnnouncements.add(reporterConfig);
		}
	}

	private void readEdgeReporterAnnouncements(DataInput in) throws IOException {
		this.edgeReporterAnnouncements = new LinkedList<EdgeQosReporterConfig>();
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			EdgeQosReporterConfig reporterConfig = new EdgeQosReporterConfig();
			reporterConfig.read(in);
			this.edgeReporterAnnouncements.add(reporterConfig);
		}
	}

	private void readChannelLatencies(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			QosReporterID.Edge reporterID = new QosReporterID.Edge();
			reporterID.read(in);

			ChannelLatency channelLatency = new ChannelLatency(reporterID,
					in.readDouble());
			this.getOrCreateChannelLatencyMap().put(reporterID, channelLatency);
		}
	}

	private void readOutputChannelStatistics(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			QosReporterID.Edge reporterID = new QosReporterID.Edge();
			reporterID.read(in);

			OutputChannelStatistics channelStats = new OutputChannelStatistics(
					reporterID, in.readDouble(), in.readDouble(),
					in.readDouble(), in.readDouble());
			this.getOrCreateOutputChannelStatsMap().put(reporterID,
					channelStats);
		}
	}

	private void readTaskLatencies(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			QosReporterID.Vertex reporterID = new QosReporterID.Vertex();
			reporterID.read(in);

			TaskLatency taskLatency = new TaskLatency(reporterID,
					in.readDouble());
			this.getOrCreateTaskLatencyMap().put(reporterID, taskLatency);
		}
	}

	public boolean isEmpty() {
		return this.channelLatencies == null && this.outputChannelStats == null
				&& this.taskLatencies == null
				&& this.vertexReporterAnnouncements == null
				&& this.edgeReporterAnnouncements == null;
	}
}
