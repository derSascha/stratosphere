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

package eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining;

import eu.stratosphere.nephele.execution.Mapper;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamInputGate;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamOutputGate;
import eu.stratosphere.nephele.types.Record;

public final class RuntimeChainLink {

	private final Mapper<? extends Record, ? extends Record> mapper;

	private final StreamInputGate<? extends Record> inputGate;

	private final StreamOutputGate<? extends Record> outputGate;

	public RuntimeChainLink(final Mapper<? extends Record, ? extends Record> mapper,
			final StreamInputGate<? extends Record> inputGate,
			final StreamOutputGate<? extends Record> outputGate) {

		this.mapper = mapper;
		this.inputGate = inputGate;
		this.outputGate = outputGate;
	}

	public Mapper<? extends Record, ? extends Record> getMapper() {

		return this.mapper;
	}

	public StreamInputGate<? extends Record> getInputGate() {

		return this.inputGate;
	}

	public StreamOutputGate<? extends Record> getOutputGate() {

		return this.outputGate;
	}
}
