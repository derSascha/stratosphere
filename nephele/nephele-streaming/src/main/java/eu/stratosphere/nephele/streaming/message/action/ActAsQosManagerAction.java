package eu.stratosphere.nephele.streaming.message.action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingSequence;

/**
 * This message instructs a task manager to act as a QoS manager for the given
 * profiling sequence. It contains the tasks and channels for which the QoS
 * manager shall be responsible.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class ActAsQosManagerAction extends AbstractAction {

	private ProfilingSequence profilingSequence;

	public ActAsQosManagerAction(JobID jobID,
			ProfilingSequence profilingSequence) {
		super(jobID);
		this.profilingSequence = profilingSequence;
	}

	public ActAsQosManagerAction() {
	}

	public ProfilingSequence getProfilingSequence() {
		return this.profilingSequence;
	}

	public void setProfilingSequence(ProfilingSequence profilingSequence) {
		this.profilingSequence = profilingSequence;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		this.profilingSequence.write(out);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		this.profilingSequence = new ProfilingSequence();
		this.profilingSequence.read(in);
	}
}
