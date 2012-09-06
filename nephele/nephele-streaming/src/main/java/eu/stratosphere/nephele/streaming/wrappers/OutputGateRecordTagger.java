package eu.stratosphere.nephele.streaming.wrappers;

import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.streaming.StreamingTag;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

public class OutputGateRecordTagger<T extends Record> {

	private StreamListener streamListener;

	private int[] recordsSinceLastTag;

	private OutputGate<T> outputGate;

	public OutputGateRecordTagger(OutputGate<T> outputGate, StreamListener streamListener) {
		this.outputGate = outputGate;
		this.streamListener = streamListener;
	}

	public void tagRecordIfNecessary(AbstractTaggableRecord record) {
		if (recordsSinceLastTag == null) {
			this.recordsSinceLastTag = new int[outputGate.getNumberOfOutputChannels()];
		}

		@SuppressWarnings("unchecked")
		int outputChannel = outputGate.getChannelSelector().selectChannels((T) record,
			outputGate.getNumberOfOutputChannels())[0];

		recordsSinceLastTag[outputChannel]++;

		if (recordsSinceLastTag[outputChannel] >= streamListener.getContext().getTaggingInterval()) {
			record.setTag(createTag(System.currentTimeMillis()));
			recordsSinceLastTag[outputChannel] = 0;
		} else {
			record.setTag(null);
		}
	}

	private StreamingTag createTag(final long timestamp) {
		StreamingTag tag = new StreamingTag(streamListener.getContext().getVertexID());
		tag.setTimestamp(timestamp);
		return tag;
	}
}
