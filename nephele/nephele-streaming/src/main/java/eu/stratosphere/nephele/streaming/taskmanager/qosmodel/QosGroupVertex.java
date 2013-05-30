package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.util.ArrayList;
import java.util.HashSet;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.util.SparseDelegateIterable;

public class QosGroupVertex {

	private final String name;

	/**
	 * The ID of the job vertex which is represented by this group vertex.
	 */
	private final JobVertexID jobVertexID;

	/**
	 * The list of {@link ProfilingVertex} contained in this group vertex.
	 */
	private ArrayList<QosVertex> groupMembers;

	/**
	 * The a group edge which originates from this group vertex.
	 */
	private ArrayList<QosGroupEdge> forwardEdges;

	/**
	 * The number of output gates that the member vertices have. This is equal
	 * to the number of non-null entries in forwardEdges.
	 */
	private int noOfOutputGates;

	/**
	 * The a group edge which arrives at this group vertex.
	 */
	private ArrayList<QosGroupEdge> backwardEdges;

	/**
	 * The number of input gates that the member vertices have. This is equal to
	 * the number of non-null entries in backwardEdges.
	 */
	private int noOfInputGates;

	private int noOfExecutingInstances;

	public QosGroupVertex(JobVertexID jobVertexID, String name) {
		this.name = name;
		this.jobVertexID = jobVertexID;
		this.noOfExecutingInstances = -1;
		this.groupMembers = new ArrayList<QosVertex>();
		this.forwardEdges = new ArrayList<QosGroupEdge>();
		this.backwardEdges = new ArrayList<QosGroupEdge>();
	}

	public QosGroupEdge getForwardEdge(int outputGateIndex) {
		try {
			return this.forwardEdges.get(outputGateIndex);
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}

	public void setForwardEdge(QosGroupEdge forwardEdge) {
		int outputGate = forwardEdge.getOutputGateIndex();
		if (outputGate >= this.forwardEdges.size()) {
			fillWithNulls(this.forwardEdges, outputGate + 1);
		}

		if (this.forwardEdges.get(outputGate) == null) {
			this.noOfOutputGates++;
		}
		this.forwardEdges.set(outputGate, forwardEdge);
	}

	private <T> void fillWithNulls(ArrayList<T> list, int targetSize) {
		int toAdd = targetSize - list.size();

		for (int i = 0; i < toAdd; i++) {
			list.add(null);
		}
	}

	public QosGroupEdge getBackwardEdge(int inputGateIndex) {
		try {
			return this.backwardEdges.get(inputGateIndex);
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}

	public void setBackwardEdge(QosGroupEdge backwardEdge) {
		int inputGate = backwardEdge.getInputGateIndex();
		if (inputGate >= this.backwardEdges.size()) {
			fillWithNulls(this.backwardEdges, inputGate + 1);
		}

		if (this.backwardEdges.get(inputGate) == null) {
			this.noOfInputGates++;
		}

		this.backwardEdges.set(inputGate, backwardEdge);
	}

	public String getName() {
		return this.name;
	}

	public JobVertexID getJobVertexID() {
		return this.jobVertexID;
	}

	public ArrayList<QosVertex> getMembers() {
		return this.groupMembers;
	}

	public void addGroupMember(QosVertex groupMember) {
		groupMember.setMemberIndex(this.groupMembers.size());
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
		for (QosVertex memberVertex : this.getMembers()) {
			instances.add(memberVertex.getExecutingInstance());
		}
		this.noOfExecutingInstances = instances.size();
	}

	@Override
	public String toString() {
		return this.name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.jobVertexID.hashCode();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QosGroupVertex other = (QosGroupVertex) obj;
		return this.jobVertexID.equals(other.jobVertexID);
	}

	public int getNumberOfOutputGates() {
		return this.noOfOutputGates;
	}

	public Iterable<QosGroupEdge> getForwardEdges() {
		return new SparseDelegateIterable<QosGroupEdge>(
				this.forwardEdges.iterator());
	}

	public Iterable<QosGroupEdge> getBackwardEdges() {
		return new SparseDelegateIterable<QosGroupEdge>(
				this.backwardEdges.iterator());
	}

	public int getNumberOfInputGates() {
		return this.noOfInputGates;
	}

	public boolean hasOutputGate(int outputGateIndex) {
		return this.getForwardEdge(outputGateIndex) != null;
	}

	public boolean hasInputGate(int inputGateIndex) {
		return this.getBackwardEdge(inputGateIndex) != null;
	}

	public QosVertex getMember(int i) {
		return this.groupMembers.get(i);
	}

	public int getNumberOfMembers() {
		return this.groupMembers.size();
	}

	/**
	 * Clones this group vertex including members but excluding any group edges
	 * or member gates.
	 * 
	 * @return The cloned object
	 */
	public QosGroupVertex cloneWithoutEdges() {
		QosGroupVertex clone = new QosGroupVertex(this.jobVertexID, this.name);
		clone.noOfExecutingInstances = this.noOfExecutingInstances;
		clone.groupMembers = new ArrayList<QosVertex>();
		for (QosVertex member : this.groupMembers) {
			clone.addGroupMember(member.cloneWithoutGates());
		}
		return clone;
	}
}
