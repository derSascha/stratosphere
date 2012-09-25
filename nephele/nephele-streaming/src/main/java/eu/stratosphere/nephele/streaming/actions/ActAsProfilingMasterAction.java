package eu.stratosphere.nephele.streaming.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingSequence;

public class ActAsProfilingMasterAction extends AbstractAction {

	private ProfilingSequence profilingSequence;

	public ActAsProfilingMasterAction(JobID jobID, ProfilingSequence profilingSequence) {
		super(jobID);
		this.profilingSequence = profilingSequence;
	}

	public ActAsProfilingMasterAction() {
	}

	public ProfilingSequence getProfilingSequence() {
		return profilingSequence;
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
		profilingSequence.write(out);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		this.profilingSequence = new ProfilingSequence();
		profilingSequence.read(in);
	}
}
