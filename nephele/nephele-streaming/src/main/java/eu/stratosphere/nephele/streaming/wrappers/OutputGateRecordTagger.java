package eu.stratosphere.nephele.streaming.wrappers;

import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.streaming.TimestampTag;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

public class OutputGateRecordTagger<T extends Record> {

	private StreamListener streamListener;

	private int[] recordsSinceLastTag;

	private OutputGate<T> outputGate;

	public OutputGateRecordTagger(OutputGate<T> outputGate,
			StreamListener streamListener) {
		this.outputGate = outputGate;
		this.streamListener = streamListener;
	}

	public void tagRecordIfNecessary(AbstractTaggableRecord record) {
		if (this.recordsSinceLastTag == null) {
			this.recordsSinceLastTag = new int[this.outputGate
					.getNumberOfOutputChannels()];
		}

		@SuppressWarnings("unchecked")
		int outputChannel = this.outputGate.getChannelSelector()
				.selectChannels((T) record,
						this.outputGate.getNumberOfOutputChannels())[0];

		this.recordsSinceLastTag[outputChannel]++;

		if (this.recordsSinceLastTag[outputChannel] >= this.streamListener
				.getContext().getTaggingInterval()) {
			this.tagRecord(record);
			this.recordsSinceLastTag[outputChannel] = 0;
		} else {
			record.setTag(null);
		}
	}

	private void tagRecord(AbstractTaggableRecord record) {
		TimestampTag tag = new TimestampTag();
		tag.setTimestamp(System.currentTimeMillis());
		record.setTag(tag);
	}
}
