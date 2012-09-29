package eu.stratosphere.nephele.streaming.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

public class StreamingChainAnnounce extends AbstractStreamingData {

	private ExecutionVertexID chainBeginVertexID;

	private ExecutionVertexID chainEndVertexID;

	public StreamingChainAnnounce(JobID jobID, ExecutionVertexID chainBeginVertexID, ExecutionVertexID chainEndVertexID) {
		super(jobID);
		this.chainBeginVertexID = chainBeginVertexID;
		this.chainEndVertexID = chainEndVertexID;
	}

	public ExecutionVertexID getChainBeginVertexID() {
		return chainBeginVertexID;
	}

	public ExecutionVertexID getChainEndVertexID() {
		return chainEndVertexID;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		chainBeginVertexID.write(out);
		chainEndVertexID.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		chainBeginVertexID = new ExecutionVertexID();
		chainBeginVertexID.read(in);

		chainEndVertexID = new ExecutionVertexID();
		chainEndVertexID.read(in);
	}
}
