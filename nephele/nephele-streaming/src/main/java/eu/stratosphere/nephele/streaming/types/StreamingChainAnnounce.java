package eu.stratosphere.nephele.streaming.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

public class StreamingChainAnnounce extends AbstractStreamingData {

	private ExecutionVertexID chainBeginVertexID;

	private ExecutionVertexID chainEndVertexID;

	public StreamingChainAnnounce(JobID jobID,
			ExecutionVertexID chainBeginVertexID,
			ExecutionVertexID chainEndVertexID) {
		super(jobID);
		this.chainBeginVertexID = chainBeginVertexID;
		this.chainEndVertexID = chainEndVertexID;
	}

	public ExecutionVertexID getChainBeginVertexID() {
		return this.chainBeginVertexID;
	}

	public ExecutionVertexID getChainEndVertexID() {
		return this.chainEndVertexID;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		this.chainBeginVertexID.write(out);
		this.chainEndVertexID.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		this.chainBeginVertexID = new ExecutionVertexID();
		this.chainBeginVertexID.read(in);

		this.chainEndVertexID = new ExecutionVertexID();
		this.chainEndVertexID.read(in);
	}
}
