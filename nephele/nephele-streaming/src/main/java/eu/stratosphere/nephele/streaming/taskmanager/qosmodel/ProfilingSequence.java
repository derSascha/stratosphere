package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;

public class ProfilingSequence implements IOReadableWritable {

	private ArrayList<QosGroupVertex> sequenceVertices;

	private boolean includeStartVertex;

	private boolean includeEndVertex;

	private InstanceConnectionInfo qosManager;

	public ProfilingSequence() {
		this.sequenceVertices = new ArrayList<QosGroupVertex>();
	}

	public InstanceConnectionInfo getQosManager() {
		return this.qosManager;
	}

	public void setQosManager(InstanceConnectionInfo qosManager) {
		this.qosManager = qosManager;
	}

	public List<QosGroupVertex> getSequenceVertices() {
		return this.sequenceVertices;
	}

	public void addSequenceVertex(QosGroupVertex vertex) {
		this.sequenceVertices.add(vertex);
	}

	public boolean isIncludeStartVertex() {
		return this.includeStartVertex;
	}

	public void setIncludeStartVertex(boolean includeStartVertex) {
		this.includeStartVertex = includeStartVertex;
	}

	public boolean isIncludeEndVertex() {
		return this.includeEndVertex;
	}

	public void setIncludeEndVertex(boolean includeEndVertex) {
		this.includeEndVertex = includeEndVertex;
	}

	public ProfilingSequence cloneWithoutGroupMembers() {
		ProfilingSequence cloned = new ProfilingSequence();
		cloned.setIncludeStartVertex(this.includeStartVertex);
		cloned.setIncludeEndVertex(this.includeEndVertex);

		// FIXME
//		for (int i = 0; i < this.sequenceVertices.size(); i++) {
//			QosGroupVertex vertexToClone = this.sequenceVertices.get(i);
//			QosGroupVertex clonedVertex = new QosGroupVertex(
//					vertexToClone.getJobVertexID(), vertexToClone.getName());
//
//			if (i > 0) {
//				QosGroupEdge edgeToClone = vertexToClone
//						.getBackwardEdge();
//				QosGroupEdge clonedEdge = new QosGroupEdge(
//						edgeToClone.getDistributionPattern(),
//						cloned.sequenceVertices.get(i - 1), clonedVertex);
//				cloned.sequenceVertices.get(i - 1).setForwardEdge(clonedEdge);
//				clonedVertex.setBackwardEdge(clonedEdge);
//			}
//			cloned.sequenceVertices.add(clonedVertex);
//		}
		return cloned;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(this.includeStartVertex);
		out.writeBoolean(this.includeEndVertex);
		this.qosManager.write(out);
		this.writeGroupVerticesWithoutMembers(out);
		this.writeMemberVertices(out);
	}

	private void readMemberVertices(DataInput in) throws IOException {
		// FIXME
//		for (QosGroupVertex groupVertex : this.sequenceVertices) {
//			int memberCount = in.readInt();
//			for (int i = 0; i < memberCount; i++) {
//				String name = in.readUTF();
//				ExecutionVertexID vertexID = new ExecutionVertexID();
//				vertexID.read(in);
//				QosVertex member = new QosVertex(vertexID, name);
//				InstanceConnectionInfo profilingDataSource = new InstanceConnectionInfo();
//				profilingDataSource.read(in);
//				member.setQosReporter(profilingDataSource);
//				groupVertex.getMembers().add(member);
//			}
//		}
//
//		for (int i = 0; i < this.sequenceVertices.size(); i++) {
//			ArrayList<QosVertex> members = this.sequenceVertices.get(i)
//					.getMembers();
//			for (int j = 0; j < members.size(); j++) {
//				int numberOfForwardEdges = in.readInt();
//				QosVertex sourceVertex = members.get(j);
//
//				for (int k = 0; k < numberOfForwardEdges; k++) {
//					ChannelID sourceChannelID = new ChannelID();
//					sourceChannelID.read(in);
//					ChannelID targetChannelID = new ChannelID();
//					targetChannelID.read(in);
//					QosVertex targetVertex = this.sequenceVertices
//							.get(i + 1).getMembers().get(in.readInt());
//					QosEdge edge = new QosEdge(sourceChannelID,
//							targetChannelID);
//					edge.setSourceVertex(sourceVertex);
//					edge.setSourceVertexEdgeIndex(sourceVertex
//							.getForwardEdges().size());
//					sourceVertex.addForwardEdge(edge);
//					edge.setTargetVertex(targetVertex);
//					edge.setTargetVertexEdgeIndex(targetVertex
//							.getBackwardEdges().size());
//					targetVertex.addBackwardEdge(edge);
//				}
//			}
//		}
	}

	private void writeMemberVertices(DataOutput out) throws IOException {
		// FIXME
//		HashMap<ExecutionVertexID, Integer> id2MemberPosition = new HashMap<ExecutionVertexID, Integer>();
//
//		for (QosGroupVertex groupVertex : this.sequenceVertices) {
//			List<QosVertex> members = groupVertex.getMembers();
//			out.writeInt(members.size());
//			for (int i = 0; i < members.size(); i++) {
//				QosVertex member = members.get(i);
//				id2MemberPosition.put(member.getID(), i);
//				out.writeUTF(member.getName());
//				member.getID().write(out);
//				member.getQosReporter().write(out);
//			}
//		}
//
//		for (QosGroupVertex groupVertex : this.sequenceVertices) {
//			for (QosVertex member : groupVertex.getMembers()) {
//				List<QosEdge> edges = member.getForwardEdges();
//				out.writeInt(edges.size());
//				for (QosEdge edge : edges) {
//					edge.getSourceChannelID().write(out);
//					edge.getTargetChannelID().write(out);
//					out.writeInt(id2MemberPosition.get(edge.getTargetVertex()
//							.getID()));
//				}
//			}
//		}
	}

	private void writeGroupVerticesWithoutMembers(DataOutput out)
			throws IOException {
		
		//FIXME
//		out.writeInt(this.sequenceVertices.size());
//		for (QosGroupVertex groupVertex : this.sequenceVertices) {
//			groupVertex.getJobVertexID().write(out);
//			out.writeUTF(groupVertex.getName());
//			if (groupVertex.getForwardEdge() != null) {
//				out.writeInt(groupVertex.getForwardEdge()
//						.getDistributionPattern().ordinal());
//			}
//		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.includeStartVertex = in.readBoolean();
		this.includeEndVertex = in.readBoolean();
		this.qosManager = new InstanceConnectionInfo();
		this.qosManager.read(in);
		this.readGroupVerticesWithoutMembers(in);
		this.readMemberVertices(in);
	}

	private void readGroupVerticesWithoutMembers(DataInput in)
			throws IOException {
		
		// FIXME
//		int numberOfGroupVertices = in.readInt();
//		this.sequenceVertices = new ArrayList<QosGroupVertex>(
//				numberOfGroupVertices);
//
//		DistributionPattern ingoingEdgeDistPattern = null;
//		for (int i = 0; i < numberOfGroupVertices; i++) {
//			if (i > 0) {
//				ingoingEdgeDistPattern = DistributionPattern.values()[in
//						.readInt()];
//			}
//
//			JobVertexID vertexID = new JobVertexID();
//			vertexID.read(in);
//			QosGroupVertex groupVertex = new QosGroupVertex(
//					vertexID, in.readUTF());
//			this.sequenceVertices.add(groupVertex);
//
//			if (i > 0) {
//				QosGroupVertex lastVertex = this.sequenceVertices
//						.get(i - 1);
//				QosGroupEdge groupEdge = new QosGroupEdge(
//						ingoingEdgeDistPattern, lastVertex, groupVertex);
//				lastVertex.setForwardEdge(groupEdge);
//				groupVertex.setBackwardEdge(groupEdge);
//			}
//		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ProfilingSequence(");
		for (int i = 0; i < this.sequenceVertices.size(); i++) {
			builder.append(this.sequenceVertices.get(i).getName());
			if (i < this.sequenceVertices.size() - 1) {
				builder.append("->");
			}
		}
		builder.append(")");
		return builder.toString();
	}
}
