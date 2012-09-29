package eu.stratosphere.nephele.streaming.profiling;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingEdge;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingGroupVertex;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingSequence;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingVertex;
import eu.stratosphere.nephele.streaming.profiling.ng.ProfilingSequenceSummary;
import eu.stratosphere.nephele.streaming.types.StreamingChainAnnounce;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.profiling.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.types.profiling.TaskLatency;

public class ProfilingModel {
	private Log LOG = LogFactory.getLog(ProfilingModel.class);

	private ProfilingSequence profilingGroupSequence;

	private HashMap<ExecutionVertexID, VertexLatency> vertexLatencies;

	private HashMap<ChannelID, EdgeCharacteristics> edgeCharacteristics;

	private int noOfProfilingSequences;

	public ProfilingModel(ProfilingSequence profilingGroupSequence) {
		this.profilingGroupSequence = profilingGroupSequence;
		initMaps();
		countProfilingSequences();
	}

	private void countProfilingSequences() {
		this.noOfProfilingSequences = -1;
		for (ProfilingGroupVertex groupVertex : profilingGroupSequence.getSequenceVertices()) {
			if (this.noOfProfilingSequences == -1) {
				this.noOfProfilingSequences = groupVertex.getGroupMembers().size();
			} else if (groupVertex.getBackwardEdge().getDistributionPattern() == DistributionPattern.BIPARTITE) {
				this.noOfProfilingSequences *= groupVertex.getGroupMembers().size();
			}
		}
		LOG.info(String.format("Profiling model with %d profiling sequences", this.noOfProfilingSequences));
	}

	private void initMaps() {
		this.vertexLatencies = new HashMap<ExecutionVertexID, VertexLatency>();
		this.edgeCharacteristics = new HashMap<ChannelID, EdgeCharacteristics>();

		for (ProfilingGroupVertex groupVertex : this.profilingGroupSequence.getSequenceVertices()) {
			for (ProfilingVertex vertex : groupVertex.getGroupMembers()) {
				VertexLatency vertexLatency = new VertexLatency(vertex);
				vertex.setVertexLatency(vertexLatency);
				this.vertexLatencies.put(vertex.getID(), vertexLatency);
				for (ProfilingEdge edge : vertex.getForwardEdges()) {
					EdgeCharacteristics currentEdgeChars = new EdgeCharacteristics(edge);
					edge.setEdgeCharacteristics(currentEdgeChars);
					this.edgeCharacteristics.put(edge.getSourceChannelID(), currentEdgeChars);
				}
			}
		}
	}

	public void refreshEdgeLatency(long timestamp, ChannelLatency channelLatency) {
		// FIXME workaround for bug that causes NaNs
		if (Double.isInfinite(channelLatency.getChannelLatency()) || Double.isNaN(channelLatency.getChannelLatency())) {
			return;
		}

		this.edgeCharacteristics.get(channelLatency.getSourceChannelID()).addLatencyMeasurement(timestamp,
			channelLatency.getChannelLatency());
	}

	public void refreshTaskLatency(long timestamp, TaskLatency taskLatency) {
		// FIXME workaround for bug that causes NaNs
		if (Double.isInfinite(taskLatency.getTaskLatency()) || Double.isNaN(taskLatency.getTaskLatency())) {
			return;
		}

		this.vertexLatencies.get(taskLatency.getVertexID()).addLatencyMeasurement(timestamp,
			taskLatency.getTaskLatency());
	}

	public void refreshChannelThroughput(long timestamp, ChannelThroughput channelThroughput) {
		// FIXME workaround for bug that causes NaNs
		if (Double.isInfinite(channelThroughput.getThroughput()) || Double.isNaN(channelThroughput.getThroughput())) {
			return;
		}

		this.edgeCharacteristics.get(channelThroughput.getSourceChannelID()).addThroughputMeasurement(timestamp,
			channelThroughput.getThroughput());
	}

	public void refreshChannelOutputBufferLatency(long timestamp, OutputBufferLatency latency) {
		this.edgeCharacteristics.get(latency.getSourceChannelID()).addOutputBufferLatencyMeasurement(timestamp,
			latency.getBufferLatency());
	}

	public ProfilingSequenceSummary computeProfilingSummary() {
		return new ProfilingSequenceSummary(profilingGroupSequence);
	}

	public void announceStreamingChain(StreamingChainAnnounce announce) {

		ProfilingVertex currentVertex = this.vertexLatencies.get(announce.getChainBeginVertexID()).getVertex();

		while (!currentVertex.getID().equals(announce.getChainEndVertexID())) {
			currentVertex.getForwardEdges().get(0).getEdgeCharacteristics().setIsInChain(true);
			currentVertex = currentVertex.getBackwardEdges().get(0).getTargetVertex();
		}
	}

	public ProfilingSequence getProfilingSequence() {
		return this.profilingGroupSequence;
	}
}
