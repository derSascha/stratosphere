package eu.stratosphere.nephele.streaming.profiling;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import eu.stratosphere.nephele.streaming.StreamingTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingGroupEdge;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingGroupVertex;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingSequence;
import eu.stratosphere.nephele.streaming.profiling.ng.ProfilingSequenceSummary;

public class ProfilingLogger {

	/**
	 * Provides access to the configuration entry which defines the log file location.
	 */
	private static final String PROFILING_LOGFILE_KEY = "streaming.profilingmaster.logging.profilingfile";

	private static final String DEFAULT_LOGFILE = "/tmp/profiling_" + System.currentTimeMillis() + ".txt";

	private BufferedWriter writer;

	private boolean headersWritten;

	private long timeOfNextLogging;

	private long loggingInterval;

	public ProfilingLogger(long loggingInterval) throws IOException {

		String logFile = StreamingTaskManagerPlugin.getPluginConfiguration()
			.getString(PROFILING_LOGFILE_KEY, DEFAULT_LOGFILE);

		this.writer = new BufferedWriter(new FileWriter(logFile));

		this.loggingInterval = loggingInterval;
		this.headersWritten = false;
	}

	public boolean isLoggingNecessary(long now) {
		return now >= timeOfNextLogging;
	}

	public void logLatencies(ProfilingSequenceSummary summary) throws IOException {
		if (!headersWritten) {
			writeHeaders(summary);
		}

		StringBuilder builder = new StringBuilder();
		builder.append(getLogTimestamp());
		builder.append(';');
		builder.append(summary.getNoOfActiveSubsequences());
		builder.append(';');
		builder.append(summary.getNoOfInactiveSubsequences());
		builder.append(';');
		builder.append(summary.getAvgSubsequenceLatency());
		builder.append(';');
		builder.append(summary.getMinSubsequenceLatency());
		builder.append(';');
		builder.append(summary.getMaxSubsequenceLatency());

		for (double avgElementLatency : summary.getAvgSequenceElementLatencies()) {
			builder.append(';');
			builder.append(avgElementLatency);
		}
		builder.append('\n');
		writer.write(builder.toString());
		writer.flush();
	}

	private Object getLogTimestamp() {
		return ProfilingUtils.alignToInterval(System.currentTimeMillis(), loggingInterval);
	}

	private void writeHeaders(ProfilingSequenceSummary summary) throws IOException {
		StringBuilder builder = new StringBuilder();
		builder.append("timestamp;");
		builder.append("noOfActivePaths;");
		builder.append("noOfInactivePaths;");
		builder.append("avgTotalPathLatency;");
		builder.append("minPathLatency;");
		builder.append("maxPathLatency");

		int nextEdgeIndex = 1;

		ProfilingSequence sequence = summary.getProfilingSequence();
		List<ProfilingGroupVertex> groupVertices = sequence.getSequenceVertices();

		for (int i = 0; i < groupVertices.size(); i++) {
			ProfilingGroupVertex groupVertex = groupVertices.get(i);

			boolean includeVertex = (i == 0 && sequence.isIncludeStartVertex())
				|| (i > 0 && (i < groupVertices.size() - 1))
				|| ((i == groupVertices.size() - 1) && sequence.isIncludeEndVertex());

			if (includeVertex) {
				builder.append(';');
				builder.append(groupVertex.getName());
			}

			ProfilingGroupEdge forwardEdge = groupVertex.getForwardEdge();
			if (forwardEdge != null) {
				builder.append("edge" + nextEdgeIndex + "obl");
				builder.append(';');
				builder.append("edge" + nextEdgeIndex);
				nextEdgeIndex++;
			}
		}
		builder.append('\n');
		writer.write(builder.toString());
		headersWritten = true;
	}
}
