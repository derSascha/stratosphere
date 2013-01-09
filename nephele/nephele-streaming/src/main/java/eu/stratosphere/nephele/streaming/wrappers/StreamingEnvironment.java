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

package eu.stratosphere.nephele.streaming.wrappers;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.Mapper;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializerFactory;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.plugins.wrapper.AbstractEnvironmentWrapper;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.types.Record;

/**
 * A streaming environment wraps the created input and output gates in special
 * {@link StreamingInputGate} and {@link StreamingOutputGate} objects to
 * intercept particular methods calls necessary for the statistics collection.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class StreamingEnvironment extends AbstractEnvironmentWrapper {

	private final StreamListener streamListener;

	private final List<StreamingInputGate<? extends Record>> streamingInputGates = new ArrayList<StreamingInputGate<? extends Record>>();

	private final List<StreamingOutputGate<? extends Record>> streamingOutputGates = new ArrayList<StreamingOutputGate<? extends Record>>();

	/**
	 * Constructs a new streaming environment
	 * 
	 * @param wrappedEnvironment
	 *            the environment to be encapsulated by this streaming
	 *            environment
	 * @param streamListener
	 *            the stream listener
	 */
	StreamingEnvironment(final Environment wrappedEnvironment,
			final StreamListener streamListener) {
		super(wrappedEnvironment);

		this.streamListener = streamListener;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public <T extends Record> OutputGate<T> createOutputGate(
			final GateID gateID, final Class<T> outputClass,
			final ChannelSelector<T> selector, final boolean isBroadcast) {

		final OutputGate<? extends Record> outputGate = this
				.getWrappedEnvironment().createOutputGate(gateID, outputClass,
						selector, isBroadcast);

		final StreamingOutputGate sog = new StreamingOutputGate(outputGate,
				this.streamListener);
		this.streamingOutputGates.add(sog);

		return sog;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public <T extends Record> InputGate<T> createInputGate(final GateID gateID,
			final RecordDeserializerFactory<T> deserializer) {

		final InputGate<? extends Record> inputGate = this
				.getWrappedEnvironment().createInputGate(gateID, deserializer);

		final StreamingInputGate sig = new StreamingInputGate(inputGate,
				this.streamListener);
		this.streamingInputGates.add(sig);

		return sig;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <I extends Record, O extends Record> void registerMapper(
			final Mapper<I, O> mapper) {

		if (this.streamingInputGates.size() != 1) {
			return;
		}

		if (this.streamingOutputGates.size() != 1) {
			return;
		}

		this.streamListener.registerMapper(mapper,
				(StreamingInputGate<I>) this.streamingInputGates.get(0),
				(StreamingOutputGate<O>) this.streamingOutputGates.get(0));
	}

	@Override
	public int getNumberOfOutputChannels() {
		return this.getWrappedEnvironment().getNumberOfOutputChannels();
	}

	@Override
	public int getNumberOfInputChannels() {
		return this.getWrappedEnvironment().getNumberOfInputChannels();
	}

	@Override
	public Set<ChannelID> getOutputChannelIDs() {
		return this.getWrappedEnvironment().getOutputChannelIDs();
	}

	@Override
	public Set<ChannelID> getInputChannelIDs() {
		return this.getWrappedEnvironment().getInputChannelIDs();
	}

	@Override
	public Set<GateID> getOutputGateIDs() {
		return this.getWrappedEnvironment().getOutputGateIDs();
	}

	@Override
	public Set<GateID> getInputGateIDs() {
		return this.getWrappedEnvironment().getInputGateIDs();
	}

	@Override
	public Set<ChannelID> getOutputChannelIDsOfGate(GateID gateID) {
		return this.getWrappedEnvironment().getOutputChannelIDsOfGate(gateID);
	}

	@Override
	public Set<ChannelID> getInputChannelIDsOfGate(GateID gateID) {
		return this.getWrappedEnvironment().getInputChannelIDsOfGate(gateID);
	}
}
