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

/**
 * This class stores information about the throughput of a specific output channel.
 * 
 * @author warneke
 */
public final class ChannelThroughput extends AbstractStreamProfilingRecord {

	/**
	 * The ID of the output channel.
	 */
	private ChannelID sourceChannelID;

	/**
	 * The throughput in MBit/s.
	 */
	private double throughput;

	private int counter;

	/**
	 * Constructs a new channel throughput object.
	 * 
	 * @param jobID
	 *        the ID of the job this channel throughput object belongs to
	 * @param vertexID
	 *        the ID of the vertex which is connected to this output channel
	 * @param sourceChannelID
	 *        the ID of the output channel
	 * @param throughput
	 *        the throughput in MBit/s
	 */
	public ChannelThroughput(final ChannelID sourceChannelID,
			final double throughput) {

		if (sourceChannelID == null) {
			throw new IllegalArgumentException("Argument sourceChannelID must not be null");
		}

		if (throughput < 0.0) {
			throw new IllegalArgumentException("Argument throughput must not be positive");
		}

		this.sourceChannelID = sourceChannelID;
		this.throughput = throughput;
		this.counter = 1;
	}

	/**
	 * Default constructor for deserialization.
	 */
	public ChannelThroughput() {
	}

	/**
	 * The ID of the output channel.
	 * 
	 * @return the ID of the output channel.
	 */
	public ChannelID getSourceChannelID() {
		return this.sourceChannelID;
	}

	/**
	 * Returns the measured throughput for the channel in MBit/s.
	 * 
	 * @return the measured throughput in MBit/s.
	 */
	public double getThroughput() {
		return this.throughput / this.counter;
	}

	public void add(ChannelThroughput channelThroughput) {
		this.counter++;
		this.throughput += channelThroughput.getThroughput();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.sourceChannelID.write(out);
		out.writeDouble(getThroughput());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.sourceChannelID = new ChannelID();
		this.sourceChannelID.read(in);
		this.throughput = in.readDouble();
		this.counter = 1;
	}

	@Override
	public boolean equals(Object otherObj) {
		boolean isEqual = false;
		if (otherObj instanceof ChannelThroughput) {
			ChannelThroughput other = (ChannelThroughput) otherObj;
			isEqual = other.getSourceChannelID().equals(getSourceChannelID())
				&& (other.getThroughput() == getThroughput());
		}

		return isEqual;
	}

}
