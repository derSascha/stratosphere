package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.message.StreamChainAnnounce;
import eu.stratosphere.nephele.streaming.message.qosreport.EdgeLatency;
import eu.stratosphere.nephele.streaming.message.qosreport.EdgeStatistics;
import eu.stratosphere.nephele.streaming.message.qosreport.VertexLatency;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.EdgeQosData;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingSequence;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.VertexQosData;

public class ProfilingModel {
	private static final Log LOG = LogFactory.getLog(ProfilingModel.class);

	private ProfilingSequence profilingGroupSequence;

	private HashMap<ExecutionVertexID, VertexQosData> vertexLatencies;

	private HashMap<ChannelID, EdgeQosData> edgeCharacteristics;

	private int noOfProfilingSequences;

	public ProfilingModel(ProfilingSequence profilingGroupSequence) {
		this.profilingGroupSequence = profilingGroupSequence;
		this.initMaps();
		this.countProfilingSequences();
	}

	private void countProfilingSequences() {
		this.noOfProfilingSequences = -1;
		for (QosGroupVertex groupVertex : this.profilingGroupSequence
				.getSequenceVertices()) {
			if (this.noOfProfilingSequences == -1) {
				this.noOfProfilingSequences = groupVertex.getMembers()
						.size();
			} else if (groupVertex.getBackwardEdge().getDistributionPattern() == DistributionPattern.BIPARTITE) {
				this.noOfProfilingSequences *= groupVertex.getMembers()
						.size();
			}
		}
		LOG.info(String.format("Profiling model with %d profiling sequences",
				this.noOfProfilingSequences));
	}

	private void initMaps() {
		this.vertexLatencies = new HashMap<ExecutionVertexID, VertexQosData>();
		this.edgeCharacteristics = new HashMap<ChannelID, EdgeQosData>();

		for (QosGroupVertex groupVertex : this.profilingGroupSequence
				.getSequenceVertices()) {
			for (QosVertex vertex : groupVertex.getMembers()) {
				VertexQosData vertexLatency = new VertexQosData(vertex);
				vertex.setQosData(vertexLatency);
				this.vertexLatencies.put(vertex.getID(), vertexLatency);
				for (QosEdge edge : vertex.getForwardEdges()) {
					EdgeQosData currentEdgeChars = new EdgeQosData(
							edge);
					edge.setQosData(currentEdgeChars);
					this.edgeCharacteristics.put(edge.getSourceChannelID(),
							currentEdgeChars);
				}
			}
		}
	}

	public void refreshChannelLatency(long timestamp, EdgeLatency channelLatency) {
		this.edgeCharacteristics.get(channelLatency.getSourceChannelID())
				.addLatencyMeasurement(timestamp,
						channelLatency.getEdgeLatency());
	}

	public void refreshTaskLatency(long timestamp, VertexLatency taskLatency) {
		this.vertexLatencies.get(taskLatency.getVertexID())
				.addLatencyMeasurement(timestamp, taskLatency.getVertexLatency());
	}

	public void refreshOutputChannelStatistics(long timestamp,
			EdgeStatistics channelStats) {
		
		this.edgeCharacteristics.get(channelStats.getSourceChannelID())
				.addOutputChannelStatisticsMeasurement(timestamp, channelStats);
	}

	public ProfilingSequenceSummary computeProfilingSummary() {
		return new ProfilingSequenceSummary(this.profilingGroupSequence);
	}

	public void announceStreamingChain(StreamChainAnnounce announce) {

		QosVertex currentVertex = this.vertexLatencies.get(
				announce.getChainBeginVertexID()).getVertex();

		while (!currentVertex.getID().equals(announce.getChainEndVertexID())) {
			QosEdge forwardEdge = currentVertex.getForwardEdges().get(0);
			forwardEdge.getQosData().setIsInChain(true);
			currentVertex = forwardEdge.getTargetVertex();
		}
	}

	public ProfilingSequence getProfilingSequence() {
		return this.profilingGroupSequence;
	}
}
