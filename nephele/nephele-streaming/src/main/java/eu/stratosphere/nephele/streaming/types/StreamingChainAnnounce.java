package eu.stratosphere.nephele.streaming.types;

import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;

public class StreamingChainAnnounce extends AbstractStreamingData {

	private List<ExecutionVertexID> chainedVertices;

	public StreamingChainAnnounce(List<ExecutionVertexID> chainedVertices) {
		this.chainedVertices = chainedVertices;
	}

	public List<ExecutionVertexID> getChainedVertices() {
		return chainedVertices;
	}
}
