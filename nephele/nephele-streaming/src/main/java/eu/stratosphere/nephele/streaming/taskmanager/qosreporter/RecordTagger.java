package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import java.util.Arrays;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

public class RecordTagger {

	private QosReporterThread qosReporter;

	private int[] recordsSinceLastTag;

	public RecordTagger(QosReporterThread qosReporter, int noOfOutputChannels) {
		this.qosReporter = qosReporter;
		this.recordsSinceLastTag = new int[noOfOutputChannels];
		Arrays.fill(this.recordsSinceLastTag, 0);
	}

	public void tagRecordIfNecessary(int outputChannel,
			AbstractTaggableRecord record) {

		this.recordsSinceLastTag[outputChannel]++;

		if (this.recordsSinceLastTag[outputChannel] >= this.qosReporter
				.getConfiguration().getTaggingInterval()) {
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
