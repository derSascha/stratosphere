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

package eu.stratosphere.nephele.streaming.types.profiling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.channels.ChannelID;

public final class OutputBufferLatency extends AbstractStreamProfilingRecord {

	private ChannelID sourceChannelID;

	private double bufferLatency;

	private int counter;

	public OutputBufferLatency(final ChannelID sourceChannelID,
			final double bufferLatency) {

		if (sourceChannelID == null) {
			throw new IllegalArgumentException(
					"Argument sourceChannelID must not be null");
		}

		if (bufferLatency < 0) {
			throw new IllegalArgumentException(
					"Argument bufferLatency must be greater than or equal to zero but is "
							+ bufferLatency);
		}

		this.sourceChannelID = sourceChannelID;
		this.bufferLatency = bufferLatency;
		this.counter = 1;
	}

	public OutputBufferLatency() {
	}

	public ChannelID getSourceChannelID() {
		return this.sourceChannelID;
	}

	public double getBufferLatency() {
		return this.bufferLatency / this.counter;
	}

	public void add(OutputBufferLatency obl) {
		this.counter++;
		this.bufferLatency += obl.getBufferLatency();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.sourceChannelID.write(out);
		out.writeDouble(this.getBufferLatency());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.sourceChannelID = new ChannelID();
		this.sourceChannelID.read(in);
		this.bufferLatency = in.readDouble();
		this.counter = 1;
	}

	@Override
	public boolean equals(Object otherObj) {
		boolean isEqual = false;
		if (otherObj instanceof OutputBufferLatency) {
			OutputBufferLatency other = (OutputBufferLatency) otherObj;
			isEqual = other.getSourceChannelID().equals(
					this.getSourceChannelID())
					&& other.getBufferLatency() == this.getBufferLatency();
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
		long temp = Double.doubleToLongBits(this.bufferLatency);
		int result = prime + (int) (temp ^ temp >>> 32);
		result = prime * result + this.sourceChannelID.hashCode();
		return result;
	}

}
