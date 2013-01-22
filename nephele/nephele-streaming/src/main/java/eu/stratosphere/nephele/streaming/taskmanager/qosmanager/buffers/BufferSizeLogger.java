package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import eu.stratosphere.nephele.streaming.taskmanager.StreamTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingSequence;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingVertex;

/**
 * 
 * FIXME: this class is currently broken
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class BufferSizeLogger {

	/**
	 * Provides access to the configuration entry which defines the log file
	 * location.
	 */
	private static final String BUFFERSIZE_LOGFILE_KEY = "streaming.qosmanager.logging.buffersfile";

	private static final String DEFAULT_LOGFILE = "/tmp/buffersizes_"
			+ System.getProperty("user.name") + ".txt";

	private FileWriter out;

	private ArrayList<HashSet<ProfilingEdge>> edges;

	private long timeBase;

	public BufferSizeLogger(ProfilingSequence profilingSequence)
			throws IOException {
		this.timeBase = System.currentTimeMillis();

		String logFile = StreamTaskManagerPlugin.getPluginConfiguration()
				.getString(BUFFERSIZE_LOGFILE_KEY, DEFAULT_LOGFILE);

		this.out = new FileWriter("buffersizes.txt");
		this.initEdges(profilingSequence);
	}

	private void initEdges(ProfilingSequence profilingSequence) {
		this.edges = new ArrayList<HashSet<ProfilingEdge>>();

		int forwardEdgeCount = profilingSequence.getSequenceVertices().size() - 1;
		for (int i = 0; i < forwardEdgeCount; i++) {
			ProfilingGroupVertex groupVertex = profilingSequence
					.getSequenceVertices().get(i);
			HashSet<ProfilingEdge> edgeSet = new HashSet<ProfilingEdge>();

			for (ProfilingVertex vertex : groupVertex.getGroupMembers()) {
				for (ProfilingEdge edge : vertex.getForwardEdges()) {
					edgeSet.add(edge);
				}
			}
			this.edges.add(edgeSet);
		}
	}

	public void logBufferSizes() throws IOException {
		// FIXME
		// long timestamp = System.currentTimeMillis() - timeBase;
		//
		// StringBuilder msg = new StringBuilder();
		// msg.append(timestamp);
		//
		// for (int i = 0; i < edges.size(); i++) {
		// int bufferSizeSum = 0;
		// int noOfEdges = 0;
		//
		// for (ManagementEdge edge : edges.get(i)) {
		// BufferSizeHistory bufferSizeHistory = bufferSizes.get(edge);
		// bufferSizeSum += bufferSizeHistory.getLastEntry().getBufferSize();
		// noOfEdges++;
		// }
		// int avgBufferSize = bufferSizeSum / noOfEdges;
		// msg.append(';');
		// msg.append(avgBufferSize);
		// }
		//
		// msg.append('\n');
		// out.write(msg.toString());
		// out.flush();
	}
}
