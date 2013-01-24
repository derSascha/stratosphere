package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.action.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.StreamTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.ProfilingModel;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.ProfilingSequenceSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.ProfilingSubsequenceSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.ProfilingUtils;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.EdgeCharacteristics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.VertexLatency;
import eu.stratosphere.nephele.taskmanager.bufferprovider.GlobalBufferPool;
import eu.stratosphere.nephele.util.StringUtils;

public class BufferSizeManager {

	private static final Log LOG = LogFactory.getLog(BufferSizeManager.class);

	/**
	 * Provides access to the configuration entry which defines the buffer size
	 * adjustment interval-
	 */
	private static final String QOSMANAGER_ADJUSTMENTINTERVAL_KEY = "streaming.qosmanager.adjustmentinterval";

	private static final long DEFAULT_ADJUSTMENTINTERVAL = 5000;

	private final static long WAIT_BEFORE_FIRST_ADJUSTMENT = 30 * 1000;

	public long adjustmentInterval;

	private long latencyGoal;

	private ProfilingModel profilingModel;

	private StreamMessagingThread messagingThread;

	private long timeOfNextAdjustment;

	private int maximumBufferSize;

	private BufferSizeLogger bufferSizeLogger;

	private JobID jobID;

	public BufferSizeManager(JobID jobID, long latencyGoal,
			ProfilingModel profilingModel,
			StreamMessagingThread messagingThread) {
		this.jobID = jobID;
		this.latencyGoal = latencyGoal;
		this.profilingModel = profilingModel;
		this.messagingThread = messagingThread;

		this.adjustmentInterval = StreamTaskManagerPlugin
				.getPluginConfiguration().getLong(
						QOSMANAGER_ADJUSTMENTINTERVAL_KEY,
						DEFAULT_ADJUSTMENTINTERVAL);

		this.timeOfNextAdjustment = ProfilingUtils.alignToInterval(
				System.currentTimeMillis() + WAIT_BEFORE_FIRST_ADJUSTMENT,
				this.adjustmentInterval);
		this.initBufferSizes();
		// FIXME: buffer size logging only possible for ONE profiling sequence
		// bufferSizeLogger = new
		// BufferSizeLogger(profilingModel.getProfilingSequence());
	}

	private void initBufferSizes() {
		int bufferSize = GlobalConfiguration.getInteger(
				"channel.network.bufferSizeInBytes",
				GlobalBufferPool.DEFAULT_BUFFER_SIZE_IN_BYTES);

		this.maximumBufferSize = bufferSize;

		long now = System.currentTimeMillis();
		for (ProfilingGroupVertex groupVertex : this.profilingModel
				.getProfilingSequence().getSequenceVertices()) {
			for (ProfilingVertex vertex : groupVertex.getGroupMembers()) {
				for (ProfilingEdge forwardEdge : vertex.getForwardEdges()) {
					forwardEdge.getEdgeCharacteristics().getBufferSizeHistory()
							.addToHistory(now, bufferSize);
				}
			}
		}
	}

	public long getAdjustmentInterval() {
		return this.adjustmentInterval;
	}

	HashSet<ChannelID> staleEdges = new HashSet<ChannelID>();

	public void adjustBufferSizes(ProfilingSequenceSummary summary)
			throws InterruptedException {
		HashMap<ProfilingEdge, Integer> edgesToAdjust = new HashMap<ProfilingEdge, Integer>();

		this.staleEdges.clear();
		for (ProfilingSubsequenceSummary activeSubsequence : summary
				.enumerateActiveSubsequences()) {
			if (activeSubsequence.getSubsequenceLatency() > this.latencyGoal) {
				this.collectEdgesToAdjust(activeSubsequence, edgesToAdjust);
			}
		}

		this.doAdjust(edgesToAdjust);

		LOG.info(String.format(
				"Adjusted edges: %d | Edges with stale profiling data: %d",
				edgesToAdjust.size(), this.staleEdges.size()));

		this.refreshTimeOfNextAdjustment();
	}

	public void logBufferSizes() throws IOException {
		this.bufferSizeLogger.logBufferSizes();
	}

	private void doAdjust(HashMap<ProfilingEdge, Integer> edgesToAdjust)
			throws InterruptedException {

		for (ProfilingEdge edge : edgesToAdjust.keySet()) {
			int newBufferSize = edgesToAdjust.get(edge);

			BufferSizeHistory sizeHistory = edge.getEdgeCharacteristics()
					.getBufferSizeHistory();
			sizeHistory.addToHistory(this.timeOfNextAdjustment, newBufferSize);

			this.setBufferSize(edge, newBufferSize);
		}
	}

	private void refreshTimeOfNextAdjustment() {
		long now = System.currentTimeMillis();
		while (this.timeOfNextAdjustment <= now) {
			this.timeOfNextAdjustment += this.adjustmentInterval;
		}
	}

	private void collectEdgesToAdjust(
			ProfilingSubsequenceSummary activeSubsequence,
			HashMap<ProfilingEdge, Integer> edgesToAdjust) {

		for (ProfilingEdge edge : activeSubsequence.getEdges()) {

			EdgeCharacteristics edgeChar = edge.getEdgeCharacteristics();

			if (edgesToAdjust.containsKey(edge) || edgeChar.isInChain()) {
				continue;
			}

			if (!this.hasFreshValues(edgeChar)) {
				this.staleEdges.add(edge.getSourceChannelID());
				// LOG.info("Rejecting edge due to stale values: " +
				// ProfilingUtils.formatName(edge));
				continue;
			}

			// double edgeLatency = edgeChar.getChannelLatencyInMillis();
			double outputBufferLatency = edgeChar.getOutputBufferLifetimeInMillis() / 2;
			double millisBetweenRecordEmissions = 1 / (edgeChar.getRecordsPerSecond()*1000);
						
			if (outputBufferLatency > 5 && outputBufferLatency > millisBetweenRecordEmissions) {
				this.reduceBufferSize(edgeChar, edgesToAdjust);
			} else if (outputBufferLatency <= 1 && edgeChar.getBufferSize() < this.maximumBufferSize) {
				this.increaseBufferSize(edgeChar, edgesToAdjust);
			}
		}
	}

	private void increaseBufferSize(EdgeCharacteristics edgeChar,
			HashMap<ProfilingEdge, Integer> edgesToAdjust) {
		
		int oldBufferSize = edgeChar.getBufferSize();
		int newBufferSize = Math.min(
				this.proposedIncreasedBufferSize(oldBufferSize),
				this.maximumBufferSize);

		if (this.isRelevantIncrease(oldBufferSize, newBufferSize)
				|| newBufferSize == this.maximumBufferSize) {
			edgesToAdjust.put(edgeChar.getEdge(), newBufferSize);
		}
	}

	private boolean isRelevantIncrease(int oldBufferSize, int newBufferSize) {
		return newBufferSize >= oldBufferSize + 100;
	}

	private int proposedIncreasedBufferSize(int oldBufferSize) {
		return (int) (oldBufferSize * 1.2);
	}

	private void reduceBufferSize(EdgeCharacteristics edgeChar,
			HashMap<ProfilingEdge, Integer> edgesToAdjust) {
		
		int oldBufferSize = edgeChar.getBufferSizeHistory().getLastEntry()
				.getBufferSize();
		int newBufferSize = this.proposeReducedBufferSize(edgeChar,
				oldBufferSize);

		// filters pointless minor changes in buffer size
		if (this.isRelevantReduction(newBufferSize, oldBufferSize)) {
			edgesToAdjust.put(edgeChar.getEdge(), newBufferSize);
		}

		// else {
		// LOG.info(String.format("Filtering reduction due to insignificance: %s (old:%d new:%d)",
		// ProfilingUtils.formatName(edge), oldBufferSize, newBufferSize));
		// }
	}

	private boolean isRelevantReduction(int newBufferSize, int oldBufferSize) {
		return newBufferSize < oldBufferSize * 0.98;
	}

	private int proposeReducedBufferSize(EdgeCharacteristics edgeChar,
			int oldBufferSize) {
		
		double avgOutputBufferLatency = edgeChar.getOutputBufferLifetimeInMillis() / 2;

		double reductionFactor = Math.pow(0.95, avgOutputBufferLatency);
		reductionFactor = Math.max(0.01, reductionFactor);

		int newBufferSize = (int) Math.max(50, oldBufferSize * reductionFactor);

		return newBufferSize;
	}

	private boolean hasFreshValues(EdgeCharacteristics edgeChar) {
		long freshnessThreshold = edgeChar.getBufferSizeHistory()
				.getLastEntry().getTimestamp();

		return edgeChar.isChannelLatencyFresherThan(freshnessThreshold)
				&& edgeChar.isOutputBufferLifetimeFresherThan(freshnessThreshold);
	}

	public boolean isAdjustmentNecessary(long now) {
		return now >= this.timeOfNextAdjustment;
	}

	private void setBufferSize(ProfilingEdge edge, int bufferSize)
			throws InterruptedException {
		LimitBufferSizeAction bsla = new LimitBufferSizeAction(this.jobID, edge
				.getSourceVertex().getID(), edge.getSourceChannelID(),
				bufferSize);

		if (this.profilingModel.getProfilingSequence().getQosManager()
				.equals(edge.getSourceVertex().getQosReporter())) {
			try {
				StreamTaskManagerPlugin.getInstance().sendData(bsla);
			} catch (IOException e) {
				LOG.error(StringUtils.stringifyException(e));
			}
		} else {
			this.messagingThread.sendToTaskManagerAsynchronously(edge
					.getSourceVertex().getQosReporter(), bsla);
		}
	}
}
