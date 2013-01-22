package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;

public class ProfilingVertex {

	private final ExecutionVertexID vertexID;

	private InstanceConnectionInfo qosReporter;

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
		return this.vertexID;
	}

	public List<ProfilingEdge> getForwardEdges() {
		return this.forwardEdges;
	}

	public void addForwardEdge(ProfilingEdge forwardEdge) {
		this.forwardEdges.add(forwardEdge);
	}

	public List<ProfilingEdge> getBackwardEdges() {
		return this.backwardEdges;
	}

	public void addBackwardEdge(ProfilingEdge backwardEdge) {
		this.backwardEdges.add(backwardEdge);
	}

	public InstanceConnectionInfo getQosReporter() {
		return this.qosReporter;
	}

	public void setQosReporter(InstanceConnectionInfo qosReporter) {
		this.qosReporter = qosReporter;
	}

	public VertexLatency getVertexLatency() {
		return this.vertexLatency;
	}

	public void setVertexLatency(VertexLatency vertexLatency) {
		this.vertexLatency = vertexLatency;
	}

	public String getName() {
		return this.name;
	}

	@Override
	public String toString() {
		return this.name;
	}
}
