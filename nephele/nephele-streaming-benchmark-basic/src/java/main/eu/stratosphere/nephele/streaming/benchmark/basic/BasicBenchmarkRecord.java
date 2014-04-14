package eu.stratosphere.nephele.streaming.benchmark.basic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

/**
 * @author Sascha Wolke
 */
public class BasicBenchmarkRecord extends AbstractTaggableRecord {
	private long packetIdInStream;
	private long userTaskTimings[];
	private int lastUserTaskID;
	private byte[] data;
	public static final int IDS_LENGTH =
			4 // sequence number
			+ 2*8 // job id
			+ 2*8 // source id
			+ 4; // buffer length
	
	public BasicBenchmarkRecord() {
	}
	
	public BasicBenchmarkRecord(int userTimingCount, int packageSize) throws RuntimeException {
		this.lastUserTaskID = 0;
		this.userTaskTimings = new long[userTimingCount];
		int dummySize = packageSize - IDS_LENGTH - 8 + 4 + userTimingCount*8 + 4;
		if (dummySize < 0)
			throw new RuntimeException("Package size to small!");
		this.data = new byte[dummySize];
	}
	
	public void addUserTaskTiming() {
		this.lastUserTaskID++;
		this.userTaskTimings[this.lastUserTaskID] = System.currentTimeMillis();
	}
	
	public void setFirstTiming() {
		this.userTaskTimings[0] = System.currentTimeMillis();
		this.lastUserTaskID = 0;
	}
	
	public long[] getUserTaskTimings() {
		return userTaskTimings;
	}
	
	public void setPacketIdInStream(long packetIdInStream) {
		this.packetIdInStream = packetIdInStream;
	}
	
	public void setEndOfStreamPacket() {
		this.packetIdInStream = -1;
	}

	public boolean isEndOfStreamPacket() {
		return this.packetIdInStream == -1;
	}
	
	public int getDataSize() {
		return data.length;
	}
	
	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		
		this.packetIdInStream = in.readLong();
		this.lastUserTaskID = in.readInt();
		this.userTaskTimings = new long[in.readInt()];
		for (int i = 0; i < this.userTaskTimings.length; i++)
			this.userTaskTimings[i] = in.readLong();
		this.data = new byte[in.readInt()];
		in.readFully(this.data);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		
		out.writeLong(this.packetIdInStream);
		out.writeInt(this.lastUserTaskID);
		out.writeInt(this.userTaskTimings.length);
		for (int i = 0; i < this.userTaskTimings.length; i++)
			out.writeLong(this.userTaskTimings[i]);
		out.writeInt(this.data.length);
		out.write(this.data);
	}
}
