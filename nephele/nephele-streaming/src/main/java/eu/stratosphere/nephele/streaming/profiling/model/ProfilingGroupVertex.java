package eu.stratosphere.nephele.streaming.profiling.model;

import java.util.ArrayList;
import java.util.HashSet;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobVertexID;

public class ProfilingGroupVertex {

	private final String name;

	/**
	 * The ID of the job vertex which is represented by this group vertex.
	 */
	private final JobVertexID jobVertexID;

	/**
	 * The list of {@link ProfilingVertex} contained in this group vertex.
	 */
	private ArrayList<ProfilingVertex> groupMembers = new ArrayList<ProfilingVertex>();

	/**
	 * The a group edge which originates from this group vertex.
	 */
	private ProfilingGroupEdge forwardEdge;

	/**
	 * The a group edge which arrives at this group vertex.
	 */
	private ProfilingGroupEdge backwardEdge;

	private int noOfExecutingInstances;

	public ProfilingGroupVertex(JobVertexID jobVertexID, String name) {
		this.name = name;
		this.jobVertexID = jobVertexID;
		this.noOfExecutingInstances = -1;
	}

	public ProfilingGroupEdge getForwardEdge() {
		return this.forwardEdge;
	}

	public void setForwardEdge(ProfilingGroupEdge forwardEdge) {
		this.forwardEdge = forwardEdge;
	}

	public ProfilingGroupEdge getBackwardEdge() {
		return this.backwardEdge;
	}

	public void setBackwardEdge(ProfilingGroupEdge backwardEdge) {
		this.backwardEdge = backwardEdge;
	}

	public String getName() {
		return this.name;
	}

	public JobVertexID getJobVertexID() {
		return this.jobVertexID;
	}

	public ArrayList<ProfilingVertex> getGroupMembers() {
		return this.groupMembers;
	}

	public void addGroupMember(ProfilingVertex groupMember) {
		this.groupMembers.add(groupMember);
	}

	public int getNumberOfExecutingInstances() {
		if (this.noOfExecutingInstances == -1) {
			this.countExecutingInstances();
		}

		return this.noOfExecutingInstances;
	}

	private void countExecutingInstances() {
		HashSet<InstanceConnectionInfo> instances = new HashSet<InstanceConnectionInfo>();
		for (ProfilingVertex memberVertex : this.getGroupMembers()) {
			instances.add(memberVertex.getProfilingReporter());
		}
		this.noOfExecutingInstances = instances.size();
	}

	@Override
	public String toString() {
		return this.name;
	}
}
