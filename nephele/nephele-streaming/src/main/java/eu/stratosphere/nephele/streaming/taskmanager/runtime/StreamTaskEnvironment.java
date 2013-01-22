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

package eu.stratosphere.nephele.streaming.taskmanager.runtime;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.Mapper;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializerFactory;
import eu.stratosphere.nephele.plugins.wrapper.EnvironmentWrapper;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamInputGate;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamOutputGate;
import eu.stratosphere.nephele.types.Record;

/**
 * A StreamTaskEnvironment has task-scope and wraps the created input and
 * output gates in special {@link StreamingInputGate} and
 * {@link StreamingOutputGate} objects to intercept particular methods calls
 * necessary for statistics collection.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke, Bjoern Lohrmann
 */
public final class StreamTaskEnvironment extends EnvironmentWrapper {

	private final List<StreamInputGate<? extends Record>> streamingInputGates = new ArrayList<StreamInputGate<? extends Record>>();

	private final List<StreamOutputGate<? extends Record>> streamingOutputGates = new ArrayList<StreamOutputGate<? extends Record>>();

	/**
	 * The ID of the respective execution vertex. Unfortunately the wrapped
	 * environment does not have this information.
	 */
	private ExecutionVertexID vertexID;
	
	private Mapper<? extends Record, ? extends Record> mapper;

	/**
	 * Constructs a new streaming environment
	 * 
	 * @param wrappedEnvironment
	 *            the environment to be encapsulated by this streaming
	 *            environment
	 * @param streamListener
	 *            the stream listener
	 */
	public StreamTaskEnvironment(final Environment wrappedEnvironment) {
		super(wrappedEnvironment);

	}

	/**
	 * @return the ID of the respective execution vertex this environment
	 *         belongs to.
	 */
	public ExecutionVertexID getVertexID() {
		return this.vertexID;
	}

	/**
	 * Sets the ID of the respective execution vertex this environment belongs
	 * to.
	 * 
	 * @param vertexID
	 *            the ID to set
	 */
	public void setVertexID(ExecutionVertexID vertexID) {
		if (vertexID == null)
			throw new NullPointerException("vertexID must not be null");

		this.vertexID = vertexID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T extends Record> OutputGate<T> createOutputGate(
			final GateID gateID, final Class<T> outputClass,
			final ChannelSelector<T> selector, final boolean isBroadcast) {

		OutputGate<T> outputGate = this.getWrappedEnvironment()
				.createOutputGate(gateID, outputClass, selector, isBroadcast);
		StreamOutputGate<T> sog = new StreamOutputGate<T>(outputGate);
		this.streamingOutputGates.add(sog);

		return sog;
	}
	
	public boolean isMapperTask() {
		return this.mapper != null;
	}
	
	public Mapper<? extends Record, ? extends Record> getMapper() {
		return this.mapper;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T extends Record> InputGate<T> createInputGate(final GateID gateID,
			final RecordDeserializerFactory<T> deserializer) {

		InputGate<T> inputGate = this.getWrappedEnvironment().createInputGate(
				gateID, deserializer);

		StreamInputGate<T> sig = new StreamInputGate<T>(inputGate);
		this.streamingInputGates.add(sig);

		return sig;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerMapper(Mapper<? extends Record, ? extends Record> mapper) {

		if (this.streamingInputGates.size() != 1) {
			return;
		}

		if (this.streamingOutputGates.size() != 1) {
			return;
		}
		this.mapper = mapper;
	}

	public List<StreamInputGate<? extends Record>> getInputGates() {
		return this.streamingInputGates;
	}
	
	public List<StreamOutputGate<? extends Record>> getOutputGates() {
		return this.streamingOutputGates;
	}
}
