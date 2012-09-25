package eu.stratosphere.nephele.streaming.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import eu.stratosphere.nephele.deployment.TaskDeploymentDescriptor;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.util.SerializableHashMap;
import eu.stratosphere.nephele.util.SerializableHashSet;

/**
 * Contains information usually attached to a {@link TaskDeploymentDescriptor} that tells the streaming
 * plugin on the task manager whether a certain task and which of its channels should be profiled.
 * In some situations a task may need no profiling, only some of its channels. This is indicated by an empty set
 * of {@link #taskProfilingMasters}.
 * 
 * @author Bjoern Lohrmann
 */
public class TaskProfilingInfo implements IOReadableWritable {

	private ExecutionVertexID vertexID;

	/**
	 * Where profiling information about a task should be sent. If this set is empty, this means that the task should
	 * not be profiled.
	 */
	private SerializableHashSet<InstanceConnectionInfo> taskProfilingMasters = new SerializableHashSet<InstanceConnectionInfo>();

	/**
	 * Defines for each incoming and outgoing channel, where to send profiling information. The {@link ChannelID}s in
	 * the map are those of the incoming/outgoing channel itself, not those of the "connected channels" (the other end
	 * of a channel). If there are no assigned profiling masters for a channel, then this means that the channel should
	 * not be profiled.
	 */
	private SerializableHashMap<ChannelID, SerializableHashSet<InstanceConnectionInfo>> channelProfilingMasters = new SerializableHashMap<ChannelID, SerializableHashSet<InstanceConnectionInfo>>();

	public TaskProfilingInfo(ExecutionVertexID vertexID) {
		this.vertexID = vertexID;
	}

	public TaskProfilingInfo() {
	}

	public void addTaskProfilingMaster(InstanceConnectionInfo taskProfilingMaster) {
		taskProfilingMasters.add(taskProfilingMaster);
	}

	public void addChannelProfilingMaster(ChannelID channelID, InstanceConnectionInfo channelProfilingMaster) {
		SerializableHashSet<InstanceConnectionInfo> profilingMasters = this.channelProfilingMasters.get(channelID);
		if (profilingMasters == null) {
			profilingMasters = new SerializableHashSet<InstanceConnectionInfo>();
			this.channelProfilingMasters.put(channelID, profilingMasters);
		}
		profilingMasters.add(channelProfilingMaster);
	}

	public ExecutionVertexID getVertexID() {
		return this.vertexID;
	}

	/**
	 * Returns the the set of profiling masters for a task that indicates where profiling information should be sent. If
	 * the returned set is empty, this means that the task itself should not be profiled (but some of its channel may
	 * still need to be profiled).
	 */
	public Set<InstanceConnectionInfo> getTaskProfilingMasters() {
		return taskProfilingMasters;
	}

	public Set<ChannelID> getChannelsToProfile() {
		return this.channelProfilingMasters.keySet();
	}

	/**
	 * Returns where to send profiling information of the given channel. If there are no assigned profiling masters for
	 * a channel, then this means that the channel should
	 * not be profiled.
	 * 
	 * @param channelID
	 *        A {@link ChannelID} of an incoming/outoing channel of the task (side of the channel that is local to the
	 *        task).
	 * @return A set of instance connection infos, or null if the channel should not be profiled.
	 */
	public Set<InstanceConnectionInfo> getChannelProfilingMasters(final ChannelID channelID) {
		return channelProfilingMasters.get(channelID);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.vertexID.write(out);
		this.taskProfilingMasters.write(out);
		this.channelProfilingMasters.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.vertexID = new ExecutionVertexID();
		this.vertexID.read(in);
		this.taskProfilingMasters.read(in);
		this.channelProfilingMasters.read(in);
	}
}
