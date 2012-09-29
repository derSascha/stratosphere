package eu.stratosphere.nephele.streaming.profiling.model;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.profiling.VertexLatency;

public class ProfilingVertex {

	private final ExecutionVertexID vertexID;

	private InstanceConnectionInfo profilingReporter;

	/**
	 * A list of edges originating from this vertex.
	 */
	private List<ProfilingEdge> forwardEdges = new ArrayList<ProfilingEdge>();

	/**
	 * A list of edges arriving at this vertex.
	 */
	private List<ProfilingEdge> backwardEdges = new ArrayList<ProfilingEdge>();

	private String name;

	/**
	 * Only for use on the task manager side. Will not be transferred.
	 */
	private transient VertexLatency vertexLatency;

	public ProfilingVertex(ExecutionVertexID vertexID, String name) {
		this.vertexID = vertexID;
		this.name = name;
	}

	public ExecutionVertexID getID() {
		return vertexID;
	}

	public List<ProfilingEdge> getForwardEdges() {
		return forwardEdges;
	}

	public void addForwardEdge(ProfilingEdge forwardEdge) {
		this.forwardEdges.add(forwardEdge);
	}

	public List<ProfilingEdge> getBackwardEdges() {
		return backwardEdges;
	}

	public void addBackwardEdge(ProfilingEdge backwardEdge) {
		this.backwardEdges.add(backwardEdge);
	}

	public InstanceConnectionInfo getProfilingReporter() {
		return profilingReporter;
	}

	public void setProfilingReporter(InstanceConnectionInfo profilingReporter) {
		this.profilingReporter = profilingReporter;
	}

	public VertexLatency getVertexLatency() {
		return vertexLatency;
	}

	public void setVertexLatency(VertexLatency vertexLatency) {
		this.vertexLatency = vertexLatency;
	}

	public String getName() {
		return name;
	}

	public String toString() {
		return name;
	}
}
