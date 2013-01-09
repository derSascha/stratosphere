package eu.stratosphere.nephele.streaming.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Contains information for the streaming plugin on a task manager that
 * determies which of its tasks and channels should be profiled and where the
 * profiling information is to be sent.
 * 
 * @author Bjoern Lohrmann
 */
public class StreamProfilingReporterInfo implements IOReadableWritable {

	private InstanceConnectionInfo reporterConnectionInfo;

	/**
	 * Where profiling information about a task should be sent. If this set is
	 * empty, this means that the task should not be profiled.
	 */
	private Map<ExecutionVertexID, Set<InstanceConnectionInfo>> taskProfilingMasters = new HashMap<ExecutionVertexID, Set<InstanceConnectionInfo>>();

	/**
	 * Defines for each incoming and outgoing channel, where to send profiling
	 * information. The {@link ChannelID}s in the map are those of the
	 * incoming/outgoing channel itself, not those of the "connected channels"
	 * (the other end of a channel). If there are no assigned profiling masters
	 * for a channel, then this means that the channel should not be profiled.
	 */
	private Map<ChannelID, Set<InstanceConnectionInfo>> channelProfilingMasters = new HashMap<ChannelID, Set<InstanceConnectionInfo>>();

	private JobID jobID;

	public void addTaskProfilingMaster(ExecutionVertexID vertexID,
			InstanceConnectionInfo taskProfilingMaster) {
		synchronized (this.taskProfilingMasters) {
			Set<InstanceConnectionInfo> assignedMasters = this.taskProfilingMasters
					.get(vertexID);
			if (assignedMasters == null) {
				assignedMasters = new HashSet<InstanceConnectionInfo>();
				this.taskProfilingMasters.put(vertexID, assignedMasters);
			}
			assignedMasters.add(taskProfilingMaster);
		}
	}

	public void addChannelProfilingMaster(ChannelID channelID,
			InstanceConnectionInfo channelProfilingMaster) {
		synchronized (this.channelProfilingMasters) {
			Set<InstanceConnectionInfo> assignedMasters = this.channelProfilingMasters
					.get(channelID);
			if (assignedMasters == null) {
				assignedMasters = new HashSet<InstanceConnectionInfo>();
				this.channelProfilingMasters.put(channelID, assignedMasters);
			}
			assignedMasters.add(channelProfilingMaster);
		}
	}

	public StreamProfilingReporterInfo(JobID jobID,
			InstanceConnectionInfo taskManager) {
		this.jobID = jobID;
		this.reporterConnectionInfo = taskManager;
	}

	public StreamProfilingReporterInfo() {
	}

	public InstanceConnectionInfo getReporterConnectionInfo() {
		return this.reporterConnectionInfo;
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public Collection<ExecutionVertexID> getTasksToProfile() {
		return this.taskProfilingMasters.keySet();
	}

	public Set<InstanceConnectionInfo> getTaskProfilingMasters(
			final ExecutionVertexID vertexID) {
		return this.taskProfilingMasters.get(vertexID);
	}

	public Collection<ChannelID> getChannelsToProfile() {
		return this.channelProfilingMasters.keySet();
	}

	/**
	 * Returns where to send profiling information of the given channel. If
	 * there are no assigned profiling masters for a channel, then this means
	 * that the channel should not be profiled.
	 * 
	 * @param channelID
	 *            A {@link ChannelID} of an incoming/outoing channel of the task
	 *            (side of the channel that is local to the task).
	 * @return A set of instance connection infos, or null if the channel should
	 *         not be profiled.
	 */
	public Set<InstanceConnectionInfo> getChannelProfilingMasters(
			final ChannelID channelID) {
		return this.channelProfilingMasters.get(channelID);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.jobID.write(out);
		Map<InstanceConnectionInfo, Integer> connectionInfoIds = this
				.writeConnectionInfoInfoIdMap(out);
		out.writeInt(connectionInfoIds.get(this.reporterConnectionInfo));
		this.writeTaskProfilingMasters(out, connectionInfoIds);
		this.writeChannelProfilingMasters(out, connectionInfoIds);
	}

	private void writeChannelProfilingMasters(DataOutput out,
			Map<InstanceConnectionInfo, Integer> connectionInfoIds)
			throws IOException {
		out.writeInt(this.channelProfilingMasters.size());
		for (Entry<ChannelID, Set<InstanceConnectionInfo>> entry : this.channelProfilingMasters
				.entrySet()) {
			entry.getKey().write(out);
			out.writeInt(entry.getValue().size());
			for (InstanceConnectionInfo connectionInfo : entry.getValue()) {
				out.writeInt(connectionInfoIds.get(connectionInfo));
			}
		}
	}

	private void writeTaskProfilingMasters(DataOutput out,
			Map<InstanceConnectionInfo, Integer> connectionInfoIds)
			throws IOException {
		out.writeInt(this.taskProfilingMasters.size());
		for (Entry<ExecutionVertexID, Set<InstanceConnectionInfo>> entry : this.taskProfilingMasters
				.entrySet()) {
			entry.getKey().write(out);
			out.writeInt(entry.getValue().size());
			for (InstanceConnectionInfo connectionInfo : entry.getValue()) {
				out.writeInt(connectionInfoIds.get(connectionInfo));
			}
		}
	}

	private Map<InstanceConnectionInfo, Integer> writeConnectionInfoInfoIdMap(
			DataOutput out) throws IOException {
		int nextFreeId = 0;
		Map<InstanceConnectionInfo, Integer> connectionInfoIds = new HashMap<InstanceConnectionInfo, Integer>();

		connectionInfoIds.put(this.reporterConnectionInfo, nextFreeId);
		nextFreeId++;

		for (Set<InstanceConnectionInfo> taskMasters : this.taskProfilingMasters
				.values()) {
			for (InstanceConnectionInfo taskMaster : taskMasters) {
				if (!connectionInfoIds.containsKey(taskMaster)) {
					connectionInfoIds.put(taskMaster, nextFreeId);
					nextFreeId++;
				}
			}
		}

		for (Set<InstanceConnectionInfo> channelMasters : this.channelProfilingMasters
				.values()) {
			for (InstanceConnectionInfo channelMaster : channelMasters) {
				if (!connectionInfoIds.containsKey(channelMaster)) {
					connectionInfoIds.put(channelMaster, nextFreeId);
					nextFreeId++;
				}
			}
		}

		out.writeInt(connectionInfoIds.size());
		for (Entry<InstanceConnectionInfo, Integer> entry : connectionInfoIds
				.entrySet()) {
			entry.getKey().write(out);
			out.writeInt(entry.getValue());
		}

		return connectionInfoIds;
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.jobID = new JobID();
		this.jobID.read(in);
		Map<Integer, InstanceConnectionInfo> connectionInfoIds = this
				.readConnectionInfoInfoIdMap(in);
		this.reporterConnectionInfo = connectionInfoIds.get(in.readInt());
		this.readTaskProfilingMasters(in, connectionInfoIds);
		this.readChannelProfilingMasters(in, connectionInfoIds);
	}

	private void readChannelProfilingMasters(DataInput in,
			Map<Integer, InstanceConnectionInfo> connectionInfoIds)
			throws IOException {
		int channelsToRead = in.readInt();
		for (int i = 0; i < channelsToRead; i++) {
			ChannelID channelID = new ChannelID();
			channelID.read(in);
			Set<InstanceConnectionInfo> connectionInfos = new HashSet<InstanceConnectionInfo>();
			int idsToRead = in.readInt();
			for (int j = 0; j < idsToRead; j++) {
				int connectionInfoId = in.readInt();
				connectionInfos.add(connectionInfoIds.get(connectionInfoId));
			}
			this.channelProfilingMasters.put(channelID, connectionInfos);
		}
	}

	private void readTaskProfilingMasters(DataInput in,
			Map<Integer, InstanceConnectionInfo> connectionInfoIds)
			throws IOException {

		int verticesToRead = in.readInt();
		for (int i = 0; i < verticesToRead; i++) {
			ExecutionVertexID vertexID = new ExecutionVertexID();
			vertexID.read(in);
			Set<InstanceConnectionInfo> connectionInfos = new HashSet<InstanceConnectionInfo>();
			int idsToRead = in.readInt();
			for (int j = 0; j < idsToRead; j++) {
				int connectionInfoId = in.readInt();
				connectionInfos.add(connectionInfoIds.get(connectionInfoId));
			}
			this.taskProfilingMasters.put(vertexID, connectionInfos);
		}
	}

	private Map<Integer, InstanceConnectionInfo> readConnectionInfoInfoIdMap(
			DataInput in) throws IOException {
		Map<Integer, InstanceConnectionInfo> connectionInfos = new HashMap<Integer, InstanceConnectionInfo>();
		int entriesToRead = in.readInt();
		for (int i = 0; i < entriesToRead; i++) {
			InstanceConnectionInfo connectionInfo = new InstanceConnectionInfo();
			connectionInfo.read(in);
			int connectionInfoId = in.readInt();
			connectionInfos.put(connectionInfoId, connectionInfo);
		}
		return connectionInfos;
	}
}
