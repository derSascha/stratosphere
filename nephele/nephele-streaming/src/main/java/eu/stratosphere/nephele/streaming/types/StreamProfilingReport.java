package eu.stratosphere.nephele.streaming.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.types.IdMapper.IDFactory;
import eu.stratosphere.nephele.streaming.types.IdMapper.MapperMode;
import eu.stratosphere.nephele.streaming.types.profiling.AbstractStreamProfilingRecord;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.profiling.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.types.profiling.TaskLatency;

/**
 * Holds a report on {@link AbstractStreamProfilingRecord} data for a given job on the task manager. Instead of sending
 * each {@link AbstractStreamProfilingRecord} individually, they are sent in batch with special serialization routines
 * with a simple compression mechanism.
 * 
 * @author Bjoern Lohrmann
 */
public class StreamProfilingReport extends AbstractStreamingData {

	private IdMapper<ExecutionVertexID> executionVertexIDMap;

	private IdMapper<ChannelID> channelIDMap;

	private HashMap<Long, ChannelLatency> channelLatencies;

	private HashMap<Long, ChannelThroughput> channelThroughputs;

	private HashMap<Long, OutputBufferLatency> outputBufferLatencies;

	private HashMap<Integer, TaskLatency> taskLatencies;

	/**
	 * Creates and initializes StreamProfilingReport object to be used for sending/serialization.
	 * 
	 * @param jobID
	 */
	public StreamProfilingReport(JobID jobID) {
		super(jobID);
		this.executionVertexIDMap = new IdMapper<ExecutionVertexID>(MapperMode.WRITABLE, null);
		this.channelIDMap = new IdMapper<ChannelID>(MapperMode.WRITABLE, null);

		this.channelLatencies = new HashMap<Long, ChannelLatency>();
		this.channelThroughputs = new HashMap<Long, ChannelThroughput>();
		this.outputBufferLatencies = new HashMap<Long, OutputBufferLatency>();
		this.taskLatencies = new HashMap<Integer, TaskLatency>();
	}

	/**
	 * Creates and initializes StreamProfilingReport object to be used for receiving/deserialization.
	 */
	public StreamProfilingReport() {
		super();

		this.executionVertexIDMap = new IdMapper<ExecutionVertexID>(MapperMode.READABLE,
			new IDFactory<ExecutionVertexID>() {
				@Override
				public ExecutionVertexID read(DataInput in) throws IOException {
					ExecutionVertexID id = new ExecutionVertexID();
					id.read(in);
					return id;
				}
			});
		this.channelIDMap = new IdMapper<ChannelID>(MapperMode.READABLE, new IDFactory<ChannelID>() {
			@Override
			public ChannelID read(DataInput in) throws IOException {
				ChannelID id = new ChannelID();
				id.read(in);
				return id;
			}
		});

		this.channelLatencies = new HashMap<Long, ChannelLatency>();
		this.channelThroughputs = new HashMap<Long, ChannelThroughput>();
		this.outputBufferLatencies = new HashMap<Long, OutputBufferLatency>();
		this.taskLatencies = new HashMap<Integer, TaskLatency>();
	}

	public void addChannelLatency(ChannelLatency channelLatency) {
		int sourceVertexID = executionVertexIDMap.getIntID(channelLatency.getSourceVertexID());
		int sinkVertexID = executionVertexIDMap.getIntID(channelLatency.getSinkVertexID());

		long compositeID = createCompositeID(sourceVertexID, sinkVertexID);
		channelLatencies.put(compositeID, channelLatency);
	}

	public Collection<ChannelLatency> getChannelLatencies() {
		return this.channelLatencies.values();
	}

	private long createCompositeID(int firstID, int secondID) {
		long compositeID = ((long) firstID) << 32;
		return compositeID | (secondID & 0xFFFFFFFFL);
	}

	private static int getFirstIDFromCompositeID(long compositeID) {
		return (int) (compositeID >>> 32);
	}

	private static int getSecondIDFromCompositeID(long compositeID) {
		return (int) (compositeID & 0xFFFFFFFF);
	}

	public void addChannelThroughput(ChannelThroughput channelThroughput) {
		int vertexID = executionVertexIDMap.getIntID(channelThroughput.getVertexID());
		int sourceChannelID = channelIDMap.getIntID(channelThroughput.getSourceChannelID());

		long compositeID = createCompositeID(vertexID, sourceChannelID);
		channelThroughputs.put(compositeID, channelThroughput);
	}
	
	public Collection<ChannelThroughput> getChannelThroughputs() {
		return this.channelThroughputs.values();
	}

	public void addOutputBufferLatency(OutputBufferLatency outputBufferLatency) {
		int vertexID = executionVertexIDMap.getIntID(outputBufferLatency.getVertexID());
		int sourceChannelID = channelIDMap.getIntID(outputBufferLatency.getSourceChannelID());

		long compositeID = createCompositeID(vertexID, sourceChannelID);
		outputBufferLatencies.put(compositeID, outputBufferLatency);
	}
	
	public Collection<OutputBufferLatency> getOutputBufferLatencies() {
		return this.outputBufferLatencies.values();
	}

	public void addTaskLatency(TaskLatency taskLatency) {
		int vertexID = executionVertexIDMap.getIntID(taskLatency.getVertexID());
		taskLatencies.put(vertexID, taskLatency);
	}
	
	public Collection<TaskLatency> getTaskLatencies() {
		return this.taskLatencies.values();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);

		executionVertexIDMap.write(out);
		channelIDMap.write(out);
		writeChannelLatencies(out);
		writeChannelThroughputs(out);
		writeOutputBufferLatencies(out);
		writeTaskLatencies(out);
	}

	private void writeChannelLatencies(DataOutput out) throws IOException {
		out.writeInt(channelLatencies.size());
		for (Entry<Long, ChannelLatency> entry : channelLatencies.entrySet()) {
			out.writeLong(entry.getKey());
			out.writeDouble(entry.getValue().getChannelLatency());
		}
	}

	private void writeChannelThroughputs(DataOutput out) throws IOException {
		out.writeInt(channelThroughputs.size());
		for (Entry<Long, ChannelThroughput> entry : channelThroughputs.entrySet()) {
			out.writeLong(entry.getKey());
			out.writeDouble(entry.getValue().getThroughput());
		}
	}

	private void writeOutputBufferLatencies(DataOutput out) throws IOException {
		out.writeInt(outputBufferLatencies.size());
		for (Entry<Long, OutputBufferLatency> entry : outputBufferLatencies.entrySet()) {
			out.writeLong(entry.getKey());
			out.writeInt(entry.getValue().getBufferLatency());
		}
	}

	private void writeTaskLatencies(DataOutput out) throws IOException {
		out.writeInt(taskLatencies.size());
		for (Entry<Integer, TaskLatency> entry : taskLatencies.entrySet()) {
			out.writeInt(entry.getKey());
			out.writeDouble(entry.getValue().getTaskLatency());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		executionVertexIDMap.read(in);
		channelIDMap.read(in);
		readChannelLatencies(in);
		readChannelThroughputs(in);
		readOutputBufferLatencies(in);
		readTaskLatencies(in);
	}

	private void readChannelLatencies(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			long compositeID = in.readLong();
			ChannelLatency channelLatency = new ChannelLatency(
				executionVertexIDMap.getFullID(getFirstIDFromCompositeID(compositeID)),
				executionVertexIDMap.getFullID(getSecondIDFromCompositeID(compositeID)),
				in.readDouble());
			channelLatencies.put(compositeID, channelLatency);
		}
	}

	private void readChannelThroughputs(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			long compositeID = in.readLong();
			ChannelThroughput channelThroughput = new ChannelThroughput(
				executionVertexIDMap.getFullID(getFirstIDFromCompositeID(compositeID)),
				channelIDMap.getFullID(getSecondIDFromCompositeID(compositeID)),
				in.readDouble());
			channelThroughputs.put(compositeID, channelThroughput);
		}
	}

	private void readOutputBufferLatencies(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			long compositeID = in.readLong();
			OutputBufferLatency outputBufferLatency = new OutputBufferLatency(
				executionVertexIDMap.getFullID(getFirstIDFromCompositeID(compositeID)),
				channelIDMap.getFullID(getSecondIDFromCompositeID(compositeID)),
				in.readInt());
			outputBufferLatencies.put(compositeID, outputBufferLatency);
		}
	}

	private void readTaskLatencies(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			int intTaskID = in.readInt();
			TaskLatency taskLatency = new TaskLatency(executionVertexIDMap.getFullID(intTaskID), in.readDouble());
			taskLatencies.put(intTaskID, taskLatency);
		}
	}
	
	public boolean isEmpty() {
		return executionVertexIDMap.isEmpty() && channelIDMap.isEmpty();
	}
}
