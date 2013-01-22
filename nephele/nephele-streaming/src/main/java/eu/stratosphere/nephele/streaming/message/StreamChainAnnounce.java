package eu.stratosphere.nephele.streaming.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This message, when sent to the QoS manager on a task manager, announces a new
 * chain of tasks. The QoS manager then updates its internal models accordingly.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class StreamChainAnnounce extends AbstractStreamMessage {

	private ExecutionVertexID chainBeginVertexID;

	private ExecutionVertexID chainEndVertexID;

	public StreamChainAnnounce(JobID jobID,
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
