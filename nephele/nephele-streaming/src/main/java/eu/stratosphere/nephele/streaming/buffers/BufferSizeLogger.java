package eu.stratosphere.nephele.streaming.buffers;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import eu.stratosphere.nephele.managementgraph.ManagementAttachment;
import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.streaming.profiling.EdgeCharacteristics;
import eu.stratosphere.nephele.streaming.profiling.ProfilingPath;
import eu.stratosphere.nephele.streaming.profiling.ProfilingSubgraph;

public class BufferSizeLogger {

	private FileWriter out;

	private ArrayList<HashSet<ManagementEdge>> edges;
	
	private long timeBase;

	public BufferSizeLogger(ProfilingSubgraph profilingSubgraph) throws IOException {
		timeBase = System.currentTimeMillis();
		out = new FileWriter("buffersizes.txt");
		initEdges(profilingSubgraph);
	}

	private void initEdges(ProfilingSubgraph profilingSubgraph) {
		edges = new ArrayList<HashSet<ManagementEdge>>();

		int index = 0;
		for (ProfilingPath path : profilingSubgraph.getProfilingPaths()) {
			for (ManagementAttachment pathElement : path.getPathElements()) {
				if (pathElement instanceof ManagementEdge) {
					registerEdge((ManagementEdge) pathElement, index);
					index++;
				}
			}
			index = 0;
		}
	}

	private void registerEdge(ManagementEdge edge, int index) {
		if (edges.size() <= index) {
			edges.add(new HashSet<ManagementEdge>());
		}
		edges.get(index).add(edge);
	}

	public void logBufferSizes(HashMap<ManagementEdge, BufferSizeHistory> bufferSizes) throws IOException {
		long timestamp = System.currentTimeMillis() - timeBase;
		
		StringBuilder msg = new StringBuilder();
		msg.append(timestamp);

		for (int i = 0; i < edges.size(); i++) {
			int bufferSizeSum = 0;
			int noOfEdges = 0;

			for (ManagementEdge edge : edges.get(i)) {
				if (((EdgeCharacteristics) edge.getAttachment()).isActive()) {
					BufferSizeHistory bufferSizeHistory = bufferSizes.get(edge);
					bufferSizeSum += bufferSizeHistory.getLastEntry().getBufferSize();
					noOfEdges++;
				}
			}
			int avgBufferSize = bufferSizeSum / noOfEdges;
			msg.append(';');
			msg.append(avgBufferSize);
		}

		msg.append('\n');
		out.write(msg.toString());
		out.flush();
	}
}
