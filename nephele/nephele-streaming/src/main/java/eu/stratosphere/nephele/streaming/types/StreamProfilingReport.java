package eu.stratosphere.nephele.streaming.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
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
 * Holds a profiling data report meant to be shipped to a profiling master. Instead of sending
 * each {@link AbstractStreamProfilingRecord} individually, they are sent in batch with special serialization routines
 * with a simple compression mechanism (multiple occurences of the same 16 byte ID are replaced by a four byte integer
 * placeholder). Most internal fields of this class are initialized in a lazy fashion, thus (empty) instances of this
 * class have a small memory footprint.
 * 
 * @author Bjoern Lohrmann
 */
public class StreamProfilingReport extends AbstractStreamingData {

	private MapperMode mapperMode;

	private IdMapper<ExecutionVertexID> executionVertexIDMap;

	private IdMapper<ChannelID> channelIDMap;

	private HashMap<Integer, ChannelLatency> channelLatencies;

	private HashMap<Integer, ChannelThroughput> channelThroughputs;

	private HashMap<Integer, OutputBufferLatency> outputBufferLatencies;

	private HashMap<Integer, TaskLatency> taskLatencies;

	/**
	 * Creates and initializes StreamProfilingReport object to be used for sending/serialization.
	 * 
	 * @param jobID
	 */
	public StreamProfilingReport(JobID jobID) {
		super(jobID);
		this.mapperMode = MapperMode.WRITABLE;
	}

	/**
	 * Creates and initializes StreamProfilingReport object to be used for receiving/deserialization.
	 */
	public StreamProfilingReport() {
		super();
		this.mapperMode = MapperMode.READABLE;
	}

	private HashMap<Integer, ChannelLatency> getOrCreateChannelLatencyMap() {
		if (this.channelLatencies == null) {
			this.channelLatencies = new HashMap<Integer, ChannelLatency>();
		}
		return this.channelLatencies;
	}

	private HashMap<Integer, ChannelThroughput> getOrCreateChannelThroughputMap() {
		if (this.channelThroughputs == null) {
			this.channelThroughputs = new HashMap<Integer, ChannelThroughput>();
		}
		return this.channelThroughputs;
	}

	private HashMap<Integer, OutputBufferLatency> getOrCreateOutputBufferLatencyMap() {
		if (this.outputBufferLatencies == null) {
			this.outputBufferLatencies = new HashMap<Integer, OutputBufferLatency>();
		}
		return this.outputBufferLatencies;
	}

	private HashMap<Integer, TaskLatency> getOrCreateTaskLatencyMap() {
		if (this.taskLatencies == null) {
			this.taskLatencies = new HashMap<Integer, TaskLatency>();
		}
		return this.taskLatencies;
	}

	public void addChannelLatency(ChannelLatency channelLatency) {
		int sourceChannelID = getOrCreateChannelIDMap().getIntID(channelLatency.getSourceChannelID());

		ChannelLatency existing = getOrCreateChannelLatencyMap().get(sourceChannelID);
		if (existing == null) {
			getOrCreateChannelLatencyMap().put(sourceChannelID, channelLatency);
		} else {
			existing.add(channelLatency);
		}
	}

	public Collection<ChannelLatency> getChannelLatencies() {
		if (this.channelLatencies == null) {
			return Collections.emptyList();
		} else {
			return this.channelLatencies.values();
		}
	}

	public void addChannelThroughput(ChannelThroughput channelThroughput) {
		int sourceChannelID = getOrCreateChannelIDMap().getIntID(channelThroughput.getSourceChannelID());

		ChannelThroughput existing = getOrCreateChannelThroughputMap().get(sourceChannelID);
		if (existing == null) {
			getOrCreateChannelThroughputMap().put(sourceChannelID, channelThroughput);
		} else {
			existing.add(channelThroughput);
		}
	}

	public Collection<ChannelThroughput> getChannelThroughputs() {
		if (this.channelThroughputs == null) {
			return Collections.emptyList();
		} else {
			return this.channelThroughputs.values();
		}
	}

	public void addOutputBufferLatency(OutputBufferLatency outputBufferLatency) {
		int sourceChannelID = getOrCreateChannelIDMap().getIntID(outputBufferLatency.getSourceChannelID());

		OutputBufferLatency existing = getOrCreateOutputBufferLatencyMap().get(sourceChannelID);
		if (existing == null) {
			getOrCreateOutputBufferLatencyMap().put(sourceChannelID, outputBufferLatency);
		} else {
			existing.add(outputBufferLatency);
		}
	}

	public Collection<OutputBufferLatency> getOutputBufferLatencies() {
		if (this.outputBufferLatencies == null) {
			return Collections.emptyList();
		} else {
			return this.outputBufferLatencies.values();
		}
	}

	public void addTaskLatency(TaskLatency taskLatency) {
		int vertexID = getOrCreateExecutionVertexIDMap().getIntID(taskLatency.getVertexID());
		TaskLatency existing = getOrCreateTaskLatencyMap().get(vertexID);
		if (existing == null) {
			getOrCreateTaskLatencyMap().put(vertexID, taskLatency);
		} else {
			existing.add(taskLatency);
		}
	}

	public Collection<TaskLatency> getTaskLatencies() {
		if (this.taskLatencies == null) {
			return Collections.emptyList();
		} else {
			return this.taskLatencies.values();
		}
	}

	private IdMapper<ChannelID> getOrCreateChannelIDMap() {
		if (this.channelIDMap == null) {
			if (this.mapperMode == MapperMode.WRITABLE) {
				this.channelIDMap = new IdMapper<ChannelID>(MapperMode.WRITABLE, null);
			} else {
				this.channelIDMap = new IdMapper<ChannelID>(MapperMode.READABLE, new IDFactory<ChannelID>() {
					@Override
					public ChannelID read(DataInput in) throws IOException {
						ChannelID id = new ChannelID();
						id.read(in);
						return id;
					}
				});
			}
		}

		return this.channelIDMap;
	}

	private IdMapper<ExecutionVertexID> getOrCreateExecutionVertexIDMap() {
		if (this.executionVertexIDMap == null) {
			if (this.mapperMode == MapperMode.WRITABLE) {
				this.executionVertexIDMap = new IdMapper<ExecutionVertexID>(MapperMode.WRITABLE, null);
			} else {
				this.executionVertexIDMap = new IdMapper<ExecutionVertexID>(MapperMode.READABLE,
						new IDFactory<ExecutionVertexID>() {
							@Override
							public ExecutionVertexID read(DataInput in) throws IOException {
								ExecutionVertexID id = new ExecutionVertexID();
								id.read(in);
								return id;
							}
						});

			}
		}

		return this.executionVertexIDMap;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		writeExecutionVertexIDMap(out);
		writeChannelIDMap(out);
		writeChannelLatencies(out);
		writeChannelThroughputs(out);
		writeOutputBufferLatencies(out);
		writeTaskLatencies(out);
	}

	private void writeChannelIDMap(final DataOutput out) throws IOException {
		if (this.channelIDMap != null) {
			out.writeBoolean(true);
			channelIDMap.write(out);
		} else {
			out.writeBoolean(false);
		}
	}

	private void writeExecutionVertexIDMap(final DataOutput out) throws IOException {
		if (this.executionVertexIDMap != null) {
			out.writeBoolean(true);
			executionVertexIDMap.write(out);
		} else {
			out.writeBoolean(false);
		}
	}

	private void writeChannelLatencies(DataOutput out) throws IOException {
		if (this.channelLatencies != null) {
			out.writeInt(this.channelLatencies.size());
			for (Entry<Integer, ChannelLatency> entry : channelLatencies.entrySet()) {
				out.writeInt(entry.getKey());
				out.writeDouble(entry.getValue().getChannelLatency());
			}
		} else {
			out.writeInt(0);
		}
	}

	private void writeChannelThroughputs(DataOutput out) throws IOException {
		if (this.channelThroughputs != null) {
			out.writeInt(this.channelThroughputs.size());
			for (Entry<Integer, ChannelThroughput> entry : this.channelThroughputs.entrySet()) {
				out.writeInt(entry.getKey());
				out.writeDouble(entry.getValue().getThroughput());
			}
		} else {
			out.writeInt(0);
		}
	}

	private void writeOutputBufferLatencies(DataOutput out) throws IOException {
		if (this.outputBufferLatencies != null) {
			out.writeInt(this.outputBufferLatencies.size());
			for (Entry<Integer, OutputBufferLatency> entry : this.outputBufferLatencies.entrySet()) {
				out.writeInt(entry.getKey());
				out.writeDouble(entry.getValue().getBufferLatency());
			}
		} else {
			out.writeInt(0);
		}
	}

	private void writeTaskLatencies(DataOutput out) throws IOException {
		if (this.taskLatencies != null) {
			out.writeInt(this.taskLatencies.size());
			for (Entry<Integer, TaskLatency> entry : this.taskLatencies.entrySet()) {
				out.writeInt(entry.getKey());
				out.writeDouble(entry.getValue().getTaskLatency());
			}
		} else {
			out.writeInt(0);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		readExecutionVertexIDMap(in);
		readChannelIDMap(in);
		readChannelLatencies(in);
		readChannelThroughputs(in);
		readOutputBufferLatencies(in);
		readTaskLatencies(in);
	}

	private void readChannelIDMap(final DataInput in) throws IOException {
		if (in.readBoolean()) {
			getOrCreateChannelIDMap().read(in);
		}
	}

	private void readExecutionVertexIDMap(DataInput in) throws IOException {
		if (in.readBoolean()) {
			getOrCreateExecutionVertexIDMap().read(in);
		}
	}

	private void readChannelLatencies(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			int sourceChannelID = in.readInt();
			ChannelLatency channelLatency = new ChannelLatency(getOrCreateChannelIDMap().getFullID(sourceChannelID),
				in.readDouble());
			getOrCreateChannelLatencyMap().put(sourceChannelID, channelLatency);
		}
	}

	private void readChannelThroughputs(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			int sourceChannelID = in.readInt();
			ChannelThroughput channelThroughput = new ChannelThroughput(getOrCreateChannelIDMap().getFullID(
				sourceChannelID),
				in.readDouble());
			getOrCreateChannelThroughputMap().put(sourceChannelID, channelThroughput);
		}
	}

	private void readOutputBufferLatencies(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			int sourceChannelID = in.readInt();
			OutputBufferLatency outputBufferLatency = new OutputBufferLatency(getOrCreateChannelIDMap().getFullID(
				sourceChannelID),
				in.readDouble());
			getOrCreateOutputBufferLatencyMap().put(sourceChannelID, outputBufferLatency);
		}
	}

	private void readTaskLatencies(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			int intTaskID = in.readInt();
			TaskLatency taskLatency = new TaskLatency(getOrCreateExecutionVertexIDMap().getFullID(intTaskID),
				in.readDouble());
			getOrCreateTaskLatencyMap().put(intTaskID, taskLatency);
		}
	}

	public boolean isEmpty() {
		return this.executionVertexIDMap == null && this.channelIDMap == null;
	}
}
