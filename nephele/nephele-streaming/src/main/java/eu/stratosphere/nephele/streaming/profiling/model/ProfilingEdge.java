package eu.stratosphere.nephele.streaming.profiling.model;

import eu.stratosphere.nephele.io.channels.ChannelID;

public class ProfilingEdge {

	private final ChannelID sourceChannelID;

	private final ChannelID targetChannelID;

	private ProfilingVertex sourceVertex;

	private ProfilingVertex targetVertex;

	/**
	 * The index of this edge in the source vertex's list of forward edges.
	 */
	private int sourceVertexEdgeIndex;

	/**
	 * The index of this edge in the target vertex's list of backward edges.
	 */
	private int targetVertexEdgeIndex;

	public ProfilingEdge(ChannelID sourceChannelID, ChannelID targetChannelID) {
		this.sourceChannelID = sourceChannelID;
		this.targetChannelID = targetChannelID;
	}

	public ProfilingVertex getSourceVertex() {
		return sourceVertex;
	}

	public void setSourceVertex(ProfilingVertex sourceVertex) {
		this.sourceVertex = sourceVertex;
	}

	public ProfilingVertex getTargetVertex() {
		return targetVertex;
	}

	public void setTargetVertex(ProfilingVertex targetVertex) {
		this.targetVertex = targetVertex;
	}

	public ChannelID getSourceChannelID() {
		return sourceChannelID;
	}

	public ChannelID getTargetChannelID() {
		return targetChannelID;
	}

	public int getSourceVertexEdgeIndex() {
		return sourceVertexEdgeIndex;
	}

	public void setSourceVertexEdgeIndex(int sourceVertexEdgeIndex) {
		this.sourceVertexEdgeIndex = sourceVertexEdgeIndex;
	}

	public int getTargetVertexEdgeIndex() {
		return targetVertexEdgeIndex;
	}

	public void setTargetVertexEdgeIndex(int targetVertexEdgeIndex) {
		this.targetVertexEdgeIndex = targetVertexEdgeIndex;
	}
}
