package eu.stratosphere.nephele.streaming.message.action;

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
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This message instructs a task manager to act as a QoS reporter. It contains
 * the tasks and channels to profile and the {@link InstanceConnectionInfo}
 * where the profiling reports shall be sent.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class ActAsQosReporterAction extends AbstractAction {

	private InstanceConnectionInfo reporterConnectionInfo;

	/**
	 * Where profiling information about a task should be sent. If this set is
	 * empty, this means that the task should not be profiled.
	 */
	private Map<ExecutionVertexID, Set<InstanceConnectionInfo>> taskQosManager = new HashMap<ExecutionVertexID, Set<InstanceConnectionInfo>>();

	/**
	 * Defines for each incoming and outgoing channel, where to send profiling
	 * information. The {@link ChannelID}s in the map are those of the
	 * incoming/outgoing channel itself, not those of the "connected channels"
	 * (the other end of a channel). If there are no assigned profiling masters
	 * for a channel, then this means that the channel should not be profiled.
	 */
	private Map<ChannelID, Set<InstanceConnectionInfo>> channelQosManager = new HashMap<ChannelID, Set<InstanceConnectionInfo>>();

	public void addTaskQosManager(ExecutionVertexID vertexID,
			InstanceConnectionInfo taskQosManager) {
		synchronized (this.taskQosManager) {
			Set<InstanceConnectionInfo> assignedMasters = this.taskQosManager
					.get(vertexID);
			if (assignedMasters == null) {
				assignedMasters = new HashSet<InstanceConnectionInfo>();
				this.taskQosManager.put(vertexID, assignedMasters);
			}
			assignedMasters.add(taskQosManager);
		}
	}

	public void addChannelQosManager(ChannelID channelID,
			InstanceConnectionInfo channelQosManager) {
		synchronized (this.channelQosManager) {
			Set<InstanceConnectionInfo> assignedMasters = this.channelQosManager
					.get(channelID);
			if (assignedMasters == null) {
				assignedMasters = new HashSet<InstanceConnectionInfo>();
				this.channelQosManager.put(channelID, assignedMasters);
			}
			assignedMasters.add(channelQosManager);
		}
	}

	public ActAsQosReporterAction(JobID jobID,
			InstanceConnectionInfo taskManager) {
		super(jobID);
		this.reporterConnectionInfo = taskManager;
	}

	public ActAsQosReporterAction() {
	}

	public InstanceConnectionInfo getReporterConnectionInfo() {
		return this.reporterConnectionInfo;
	}

	public Collection<ExecutionVertexID> getTasksToProfile() {
		return this.taskQosManager.keySet();
	}

	public Set<InstanceConnectionInfo> getTaskQosManagers(
			final ExecutionVertexID vertexID) {
		return this.taskQosManager.get(vertexID);
	}

	public Collection<ChannelID> getChannelsToProfile() {
		return this.channelQosManager.keySet();
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
	public Set<InstanceConnectionInfo> getChannelQosManagers(
			final ChannelID channelID) {
		return this.channelQosManager.get(channelID);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		Map<InstanceConnectionInfo, Integer> connectionInfoIds = this
				.writeConnectionInfoInfoIdMap(out);
		out.writeInt(connectionInfoIds.get(this.reporterConnectionInfo));
		this.writeTaskQosManagers(out, connectionInfoIds);
		this.writeChannelQosManagers(out, connectionInfoIds);
	}

	private void writeChannelQosManagers(DataOutput out,
			Map<InstanceConnectionInfo, Integer> connectionInfoIds)
			throws IOException {
		out.writeInt(this.channelQosManager.size());
		for (Entry<ChannelID, Set<InstanceConnectionInfo>> entry : this.channelQosManager
				.entrySet()) {
			entry.getKey().write(out);
			out.writeInt(entry.getValue().size());
			for (InstanceConnectionInfo connectionInfo : entry.getValue()) {
				out.writeInt(connectionInfoIds.get(connectionInfo));
			}
		}
	}

	private void writeTaskQosManagers(DataOutput out,
			Map<InstanceConnectionInfo, Integer> connectionInfoIds)
			throws IOException {
		out.writeInt(this.taskQosManager.size());
		for (Entry<ExecutionVertexID, Set<InstanceConnectionInfo>> entry : this.taskQosManager
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

		for (Set<InstanceConnectionInfo> taskMasters : this.taskQosManager
				.values()) {
			for (InstanceConnectionInfo taskMaster : taskMasters) {
				if (!connectionInfoIds.containsKey(taskMaster)) {
					connectionInfoIds.put(taskMaster, nextFreeId);
					nextFreeId++;
				}
			}
		}

		for (Set<InstanceConnectionInfo> channelMasters : this.channelQosManager
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
		super.read(in);
		Map<Integer, InstanceConnectionInfo> connectionInfoIds = this
				.readConnectionInfoInfoIdMap(in);
		this.reporterConnectionInfo = connectionInfoIds.get(in.readInt());
		this.readTaskQosManagers(in, connectionInfoIds);
		this.readChannelQosManagers(in, connectionInfoIds);
	}

	private void readChannelQosManagers(DataInput in,
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
			this.channelQosManager.put(channelID, connectionInfos);
		}
	}

	private void readTaskQosManagers(DataInput in,
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
			this.taskQosManager.put(vertexID, connectionInfos);
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
