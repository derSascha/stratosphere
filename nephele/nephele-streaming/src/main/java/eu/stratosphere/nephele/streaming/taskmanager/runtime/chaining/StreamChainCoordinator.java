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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;

public final class StreamChainCoordinator {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory
			.getLog(StreamChainCoordinator.class);

	private final ConcurrentMap<ExecutionVertexID, StreamTaskEnvironment> chainLinks = new ConcurrentHashMap<ExecutionVertexID, StreamTaskEnvironment>();

	public void registerMapper(StreamTaskEnvironment taskEnvironment) {
		LOG.info("Registering stream chain link for vertex ID "
				+ taskEnvironment.getVertexID());
		this.chainLinks.putIfAbsent(taskEnvironment.getVertexID(),
				taskEnvironment);
	}

	public StreamChain constructStreamChain(List<ExecutionVertexID> vertexIDs) {

		ArrayList<StreamChainLink> chainLinkList = new ArrayList<StreamChainLink>();

		for (ExecutionVertexID vertexID : vertexIDs) {
			StreamTaskEnvironment taskEnvironment = this.chainLinks
					.get(vertexID);

			if (taskEnvironment == null) {
				LOG.error("Cannot construct stream chain from "
						+ vertexIDs.get(0) + " to "
						+ vertexIDs.get(vertexIDs.size() - 1)
						+ ": No chain link for vertex ID " + vertexID);
				return null;
			}

			chainLinkList.add(new StreamChainLink(taskEnvironment.getMapper(),
					taskEnvironment.getInputGate(0), taskEnvironment
							.getOutputGate(0)));
		}

		return new StreamChain(Collections.unmodifiableList(chainLinkList));
	}
}
