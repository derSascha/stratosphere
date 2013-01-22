package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

public class BufferSizeHistoryEntry {
	private int entryIndex;

	private long timestamp;

	private int bufferSize;

	public BufferSizeHistoryEntry(int entryIndex, long timestamp, int bufferSize) {
		this.entryIndex = entryIndex;
		this.timestamp = timestamp;
		this.bufferSize = bufferSize;
	}

	public int getEntryIndex() {
		return this.entryIndex;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public int getBufferSize() {
		return this.bufferSize;
	}

}
