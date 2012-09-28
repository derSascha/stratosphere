package eu.stratosphere.nephele.streaming.profiling.model;

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

	private ArrayList<ProfilingGroupVertex> sequenceVertices;

	private boolean includeStartVertex;

	private boolean includeEndVertex;

	public ProfilingSequence() {
		this.sequenceVertices = new ArrayList<ProfilingGroupVertex>();
	}

	public List<ProfilingGroupVertex> getSequenceVertices() {
		return sequenceVertices;
	}

	public void addSequenceVertex(ProfilingGroupVertex vertex) {
		this.sequenceVertices.add(vertex);
	}

	public boolean isIncludeStartVertex() {
		return includeStartVertex;
	}

	public void setIncludeStartVertex(boolean includeStartVertex) {
		this.includeStartVertex = includeStartVertex;
	}

	public boolean isIncludeEndVertex() {
		return includeEndVertex;
	}

	public void setIncludeEndVertex(boolean includeEndVertex) {
		this.includeEndVertex = includeEndVertex;
	}

	public ProfilingSequence cloneWithoutGroupMembers() {
		ProfilingSequence cloned = new ProfilingSequence();
		cloned.setIncludeStartVertex(includeStartVertex);
		cloned.setIncludeEndVertex(includeEndVertex);

		for (int i = 0; i < this.sequenceVertices.size(); i++) {
			ProfilingGroupVertex vertexToClone = this.sequenceVertices.get(i);
			ProfilingGroupVertex clonedVertex = new ProfilingGroupVertex(vertexToClone.getJobVertexID(),
				vertexToClone.getName());

			if (i > 0) {
				ProfilingGroupEdge edgeToClone = vertexToClone.getBackwardEdge();
				ProfilingGroupEdge clonedEdge = new ProfilingGroupEdge(edgeToClone.getDistributionPattern(),
					cloned.sequenceVertices.get(i - 1), clonedVertex);
				cloned.sequenceVertices.get(i - 1).setForwardEdge(clonedEdge);
				clonedVertex.setBackwardEdge(clonedEdge);
			}
			cloned.sequenceVertices.add(clonedVertex);
		}
		return cloned;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(includeStartVertex);
		out.writeBoolean(includeEndVertex);
		writeGroupVerticesWithoutMembers(out);
		writeMemberVertices(out);
	}


	private void readMemberVertices(DataInput in) throws IOException {
		for (ProfilingGroupVertex groupVertex : sequenceVertices) {
			int memberCount = in.readInt();
			for (int i = 0; i < memberCount; i++) {
				ExecutionVertexID vertexID = new ExecutionVertexID();
				vertexID.read(in);
				ProfilingVertex member = new ProfilingVertex(vertexID);
				InstanceConnectionInfo profilingDataSource = new InstanceConnectionInfo();
				profilingDataSource.read(in);
				member.setProfilingReporter(profilingDataSource);
				groupVertex.getGroupMembers().add(member);
			}
		}

		for (int i = 0; i < this.sequenceVertices.size(); i++) {
			ArrayList<ProfilingVertex> members = this.sequenceVertices.get(i).getGroupMembers();
			for (int j = 0; j < members.size(); j++) {
				int numberOfForwardEdges = in.readInt();
				ProfilingVertex sourceVertex = members.get(j);

				for (int k = 0; k < numberOfForwardEdges; k++) {
					ChannelID sourceChannelID = new ChannelID();
					sourceChannelID.read(in);
					ChannelID targetChannelID = new ChannelID();
					targetChannelID.read(in);
					ProfilingVertex targetVertex = this.sequenceVertices.get(i + 1).getGroupMembers().get(in.readInt());
					ProfilingEdge edge = new ProfilingEdge(sourceChannelID, targetChannelID);
					edge.setSourceVertex(sourceVertex);
					edge.setSourceVertexEdgeIndex(sourceVertex.getForwardEdges().size());
					sourceVertex.addForwardEdge(edge);
					edge.setTargetVertex(targetVertex);
					edge.setTargetVertexEdgeIndex(targetVertex.getBackwardEdges().size());
					targetVertex.addBackwardEdge(edge);
				}
			}
		}
	}
	
	private void writeMemberVertices(DataOutput out) throws IOException {
		HashMap<ExecutionVertexID, Integer> id2MemberPosition = new HashMap<ExecutionVertexID, Integer>();

		for (ProfilingGroupVertex groupVertex : sequenceVertices) {
			List<ProfilingVertex> members = groupVertex.getGroupMembers();
			out.writeInt(members.size());
			for (int i = 0; i < members.size(); i++) {
				ProfilingVertex member = members.get(i);
				id2MemberPosition.put(member.getID(), i);
				member.getID().write(out);
				member.getProfilingReporter().write(out);
			}
		}

		for (ProfilingGroupVertex groupVertex : sequenceVertices) {
			for (ProfilingVertex member : groupVertex.getGroupMembers()) {
				List<ProfilingEdge> edges = member.getForwardEdges();
				out.writeInt(edges.size());
				for (ProfilingEdge edge : edges) {
					edge.getSourceChannelID().write(out);
					edge.getTargetChannelID().write(out);
					out.writeInt(id2MemberPosition.get(edge.getTargetVertex().getID()));
				}
			}
		}
	}


	private void writeGroupVerticesWithoutMembers(DataOutput out) throws IOException {
		out.writeInt(sequenceVertices.size());
		for (ProfilingGroupVertex groupVertex : sequenceVertices) {
			groupVertex.getJobVertexID().write(out);
			out.writeUTF(groupVertex.getName());
			if (groupVertex.getForwardEdge() != null) {
				out.writeInt(groupVertex.getForwardEdge().getDistributionPattern().ordinal());
			}
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.includeStartVertex = in.readBoolean();
		this.includeEndVertex = in.readBoolean();
		readGroupVerticesWithoutMembers(in);
		readMemberVertices(in);
	}

	private void readGroupVerticesWithoutMembers(DataInput in) throws IOException {
		int numberOfGroupVertices = in.readInt();
		this.sequenceVertices = new ArrayList<ProfilingGroupVertex>(numberOfGroupVertices);

		DistributionPattern ingoingEdgeDistPattern = null;
		for (int i = 0; i < numberOfGroupVertices; i++) {
			if (i > 0) {
				ingoingEdgeDistPattern = DistributionPattern.values()[in.readInt()];
			}

			JobVertexID vertexID = new JobVertexID();
			vertexID.read(in);
			ProfilingGroupVertex groupVertex = new ProfilingGroupVertex(vertexID, in.readUTF());
			this.sequenceVertices.add(groupVertex);

			if (i > 0) {
				ProfilingGroupVertex lastVertex = this.sequenceVertices.get(i - 1);
				ProfilingGroupEdge groupEdge = new ProfilingGroupEdge(ingoingEdgeDistPattern, lastVertex, groupVertex);
				lastVertex.setForwardEdge(groupEdge);
				groupVertex.setBackwardEdge(groupEdge);
			}
		}
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ProfilingSequence(");
		for (int i = 0; i < sequenceVertices.size(); i++) {
			builder.append(sequenceVertices.get(i).getName());
			if (i < sequenceVertices.size() - 1) {
				builder.append("->");
			}
		}
		builder.append(")");
		return builder.toString();
	}
}
