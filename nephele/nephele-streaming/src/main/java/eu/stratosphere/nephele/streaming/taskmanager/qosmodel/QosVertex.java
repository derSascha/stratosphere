package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;

public class QosVertex {

	private QosGroupVertex groupVertex;

	private ExecutionVertexID vertexID;

	private InstanceConnectionInfo executingInstance;

	private ArrayList<QosGate> inputGates;

	private ArrayList<QosGate> outputGates;

	private int memberIndex;

	private String name;

	/**
	 * Only for use on the task manager side. Will not be transferred.
	 */
	private transient VertexQosData qosData;

	public QosVertex(ExecutionVertexID vertexID, String name,
			InstanceConnectionInfo executingInstance) {
		this.vertexID = vertexID;
		this.name = name;
		this.executingInstance = executingInstance;
		this.inputGates = new ArrayList<QosGate>();
		this.outputGates = new ArrayList<QosGate>();
		this.memberIndex = -1;
	}

	public ExecutionVertexID getID() {
		return this.vertexID;
	}

	public QosGate getInputGate(int gateIndex) {
		try {
			return this.inputGates.get(gateIndex);
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}

	public void setInputGate(QosGate inputGate) {
		if (inputGate.getGateIndex() >= this.inputGates.size()) {
			fillWithNulls(this.inputGates, inputGate.getGateIndex() + 1);
		}

		this.inputGates.set(inputGate.getGateIndex(), inputGate);
	}

	public QosGate getOutputGate(int gateIndex) {
		try {
			return this.outputGates.get(gateIndex);
		} catch (IndexOutOfBoundsException e) {
			return null;
		}
	}

	public void setOutputGate(QosGate outputGate) {
		if (outputGate.getGateIndex() >= this.outputGates.size()) {
			fillWithNulls(this.outputGates, outputGate.getGateIndex() + 1);
		}

		this.outputGates.set(outputGate.getGateIndex(), outputGate);
	}

	private <T> void fillWithNulls(ArrayList<T> list, int targetSize) {
		int toAdd = targetSize - list.size();

		for (int i = 0; i < toAdd; i++) {
			list.add(null);
		}
	}

	public InstanceConnectionInfo getExecutingInstance() {
		return this.executingInstance;
	}

	public VertexQosData getQosData() {
		return this.qosData;
	}

	public void setQosData(VertexQosData qosData) {
		this.qosData = qosData;
	}

	public String getName() {
		return this.name;
	}

	@Override
	public String toString() {
		return this.name;
	}

	public QosVertex cloneWithoutGates() {
		QosVertex clone = new QosVertex(this.vertexID, this.name,
				this.executingInstance);
		return clone;
	}

	public void setMemberIndex(int memberIndex) {
		this.memberIndex = memberIndex;
	}

	public int getMemberIndex() {
		return this.memberIndex;
	}

	public void setGroupVertex(QosGroupVertex qosGroupVertex) {
		this.groupVertex = qosGroupVertex;
	}

	public QosGroupVertex getGroupVertex() {
		return this.groupVertex;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + memberIndex;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime
				* result
				+ ((executingInstance == null) ? 0 : executingInstance
						.hashCode());
		result = prime * result
				+ ((vertexID == null) ? 0 : vertexID.hashCode());
		return result;
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
		QosVertex other = (QosVertex) obj;
		if (memberIndex != other.memberIndex)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (executingInstance == null) {
			if (other.executingInstance != null)
				return false;
		} else if (!executingInstance.equals(other.executingInstance))
			return false;
		if (vertexID == null) {
			if (other.vertexID != null)
				return false;
		} else if (!vertexID.equals(other.vertexID))
			return false;
		return true;
	}
}
