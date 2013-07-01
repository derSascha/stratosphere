/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.streaming.message.profiling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;


public final class TaskLatency extends AbstractStreamProfilingRecord {

	private QosReporterID.Vertex reporterID;
	
	private int counter;

	private double taskLatency;

	/**
	 * Constructs a new task latency object.
	 * 
	 * @param jobID
	 *            the ID of the job this path latency information refers to
	 * @param vertexID
	 *            the ID of the vertex this task latency information refers to
	 * @param taskLatency
	 *            the task latency in milliseconds
	 */
	public TaskLatency(final QosReporterID.Vertex reporterID,
			final double taskLatency) {


		this.reporterID = reporterID;
		this.taskLatency = taskLatency;
		this.counter = 1;
	}

	/**
	 * Default constructor for the deserialization of the object.
	 */
	public TaskLatency() {
	}

	public QosReporterID.Vertex getReporterID() {
		return this.reporterID;
	}

	/**
	 * Returns the task latency in milliseconds.
	 * 
	 * @return the task latency in milliseconds
	 */
	public double getTaskLatency() {
		return this.taskLatency / this.counter;
	}

	public void add(TaskLatency taskLatency) {
		this.counter++;
		this.taskLatency += taskLatency.getTaskLatency();
	}

	@Override
	public boolean equals(Object otherObj) {
		boolean isEqual = false;
		if (otherObj instanceof TaskLatency) {
			TaskLatency other = (TaskLatency) otherObj;
			isEqual = other.getReporterID().equals(this.getReporterID())
					&& other.getTaskLatency() == this.getTaskLatency();
		}

		return isEqual;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		long temp = Double.doubleToLongBits(this.taskLatency);
		int result = prime + (int) (temp ^ temp >>> 32);
		result = prime * result + this.reporterID.hashCode();
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.reporterID.write(out);
		out.writeDouble(this.getTaskLatency());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.reporterID = new QosReporterID.Vertex();
		this.reporterID.read(in);
		this.taskLatency = in.readDouble();
	}
}
