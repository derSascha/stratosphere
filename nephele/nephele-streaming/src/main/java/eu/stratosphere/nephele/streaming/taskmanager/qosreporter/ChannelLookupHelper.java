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
package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import java.util.ArrayList;
import java.util.HashMap;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamInputGate;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamOutputGate;

/**
 * @author Bjoern Lohrmann
 *
 */
public class ChannelLookupHelper {
	
	private final HashMap<GateID, Integer> registeredGates;
	
	private final ArrayList<StreamInputGate<?>> inputGates;
	
	private final ArrayList<StreamOutputGate<?>> outputGates;
	
	/**
	 * List of channel arrays. Contains the channels of each input gate
	 * for which QoS reporting is required. The array indices are identical
	 * to input channel indices in the input gate. 
	 */
	private final ArrayList<AbstractByteBufferedInputChannel<?>[]> inputGateChannels;
	
	/**
	 * List of channel arrays. Contains the channels of each output gate
	 * for which QoS reporting is required. The array indices
	 * are identical to output channel indices in the output gate.
	 */
	private final ArrayList<AbstractByteBufferedOutputChannel<?>[]> outputGateChannels;
	
	
	/**
	 * Maps the channel ID of each output channel for which QoS reporting is
	 * required to the output channel object itself.
	 */
	private final HashMap<ChannelID, AbstractByteBufferedOutputChannel<?>> outputChannelMap;
	
	public ChannelLookupHelper() {
		this.registeredGates = new HashMap<GateID, Integer>();
		this.inputGates = new ArrayList<StreamInputGate<?>>();
		this.inputGateChannels = new ArrayList<AbstractByteBufferedInputChannel<?>[]>();
		this.outputGates = new ArrayList<StreamOutputGate<?>>();
		this.outputGateChannels = new ArrayList<AbstractByteBufferedOutputChannel<?>[]>();
		this.outputChannelMap = new HashMap<ChannelID, AbstractByteBufferedOutputChannel<?>>();
	}


	public int registerInputGate(StreamInputGate<?> gate) {		
		int gateIndex = this.inputGates.size();
		this.registeredGates.put(gate.getGateID(), gateIndex);
		this.inputGates.add(gate);
		this.inputGateChannels.add(pullChannelsFromInputGate(gate));
		return gateIndex;
	}
	
	public int getGateIndex(GateID gateID) {
		return this.registeredGates.get(gateID);
	}
	
	private AbstractByteBufferedInputChannel<?>[] pullChannelsFromInputGate(
			StreamInputGate<?> inputGate) {

		int noOfChannels = inputGate.getNumberOfInputChannels();
		AbstractByteBufferedInputChannel<?>[] channels = new AbstractByteBufferedInputChannel<?>[noOfChannels];
		for (int i = 0; i < noOfChannels; i++) {
			channels[i] = (AbstractByteBufferedInputChannel<?>) inputGate
					.getInputChannel(i);
		}
		return channels;
	}
	
	public int registerChannelsFromOutputGate(StreamOutputGate<?> gate) {
		
		if(this.registeredGates.containsKey(gate.getGateID())) {
			return this.registeredGates.get(gate.getGateID());
		}
		
		int gateIndex = this.outputGates.size();
		this.registeredGates.put(gate.getGateID(), gateIndex);
		this.outputGates.add(gate);
		this.outputGateChannels.add(pullChannelsFromOutputGate(gate));
		return gateIndex;
	}

	private AbstractByteBufferedOutputChannel<?>[] pullChannelsFromOutputGate(StreamOutputGate<?> outputGate) {
		
		int noOfChannels = outputGate.getNumberOfOutputChannels();
		AbstractByteBufferedOutputChannel<?>[] channels = new AbstractByteBufferedOutputChannel<?>[noOfChannels];
		for (int i = 0; i < noOfChannels; i++) {
			channels[i] = (AbstractByteBufferedOutputChannel<?>) outputGate.getOutputChannel(i);
			this.outputChannelMap.put(channels[i].getID(), channels[i]);
		}
		return channels;
	}
	
	public AbstractByteBufferedOutputChannel<?> getOutputChannelByID(ChannelID channelID) {
		return this.outputChannelMap.get(channelID);
	}


	public AbstractByteBufferedInputChannel<?> getInputChannel(int inputGateIndex,
			int inputChannelIndex) {
		
		return this.inputGateChannels.get(inputGateIndex)[inputChannelIndex];
	}
	
	public AbstractByteBufferedOutputChannel<?> getOutputChannel(int outputGateIndex,
			int outputChannelIndex) {
		
		return this.outputGateChannels.get(outputGateIndex)[outputChannelIndex];
	}	
}
