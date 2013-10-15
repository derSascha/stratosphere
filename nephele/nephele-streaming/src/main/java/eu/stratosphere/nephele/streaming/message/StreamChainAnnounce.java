package eu.stratosphere.nephele.streaming.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

/**
 * This message, sent by the Qos reporters on a task manager to their common QoS
 * manager, announces a new chain of tasks. The QoS manager can then updates its
 * internal models accordingly.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class StreamChainAnnounce extends AbstractStreamMessage {

	private QosReporterID.Vertex chainBegin;

	private QosReporterID.Vertex chainEnd;

	public StreamChainAnnounce(JobID jobID, QosReporterID.Vertex chainBegin,
			QosReporterID.Vertex chainEnd) {
		super(jobID);
		this.chainBegin = chainBegin;
		this.chainEnd = chainEnd;
	}

	/**
	 * Returns the chainBegin.
	 * 
	 * @return the chainBegin
	 */
	public QosReporterID.Vertex getChainBegin() {
		return this.chainBegin;
	}

	/**
	 * Returns the chainEnd.
	 * 
	 * @return the chainEnd
	 */
	public QosReporterID.Vertex getChainEnd() {
		return this.chainEnd;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		this.chainBegin.write(out);
		this.chainEnd.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		this.chainBegin = new QosReporterID.Vertex();
		this.chainBegin.read(in);

		this.chainEnd = new QosReporterID.Vertex();
		this.chainEnd.read(in);
	}
}
