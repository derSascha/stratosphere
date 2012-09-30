package eu.stratosphere.nephele.streaming.buffers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.StreamingCommunicationThread;
import eu.stratosphere.nephele.streaming.StreamingTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.actions.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.profiling.EdgeCharacteristics;
import eu.stratosphere.nephele.streaming.profiling.ProfilingModel;
import eu.stratosphere.nephele.streaming.profiling.ProfilingUtils;
import eu.stratosphere.nephele.streaming.profiling.VertexLatency;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingEdge;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingGroupVertex;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingVertex;
import eu.stratosphere.nephele.streaming.profiling.ng.ProfilingSequenceSummary;
import eu.stratosphere.nephele.streaming.profiling.ng.ProfilingSubsequenceSummary;
import eu.stratosphere.nephele.taskmanager.bufferprovider.GlobalBufferPool;

public class BufferSizeManager {

	private Log LOG = LogFactory.getLog(BufferSizeManager.class);

	/**
	 * Provides access to the configuration entry which defines the buffer size adjustment interval-
	 */
	private static final String PROFILINGMASTER_ADJUSTMENTINTERVAL_KEY = "streaming.profilingmaster.adjustmentinterval";

	private static final long DEFAULT_ADJUSTMENTINTERVAL = 5000;

	private final static long WAIT_BEFORE_FIRST_ADJUSTMENT = 30 * 1000;

	public long adjustmentInterval;

	private long latencyGoal;

	private ProfilingModel profilingModel;

	private StreamingCommunicationThread communicationThread;

	private long timeOfNextAdjustment;

	private int maximumBufferSize;

	private BufferSizeLogger bufferSizeLogger;

	private JobID jobID;

	public BufferSizeManager(JobID jobID, long latencyGoal, ProfilingModel profilingModel,
			StreamingCommunicationThread communicationThread) {
		this.jobID = jobID;
		this.latencyGoal = latencyGoal;
		this.profilingModel = profilingModel;
		this.communicationThread = communicationThread;

		this.adjustmentInterval = StreamingTaskManagerPlugin.getPluginConfiguration().getLong(
			PROFILINGMASTER_ADJUSTMENTINTERVAL_KEY, DEFAULT_ADJUSTMENTINTERVAL);

		this.timeOfNextAdjustment = ProfilingUtils.alignToInterval(System.currentTimeMillis()
			+ WAIT_BEFORE_FIRST_ADJUSTMENT, this.adjustmentInterval);
		initBufferSizes();
		// FIXME: buffer size logging only possible for ONE profiling sequence
		// bufferSizeLogger = new BufferSizeLogger(profilingModel.getProfilingSequence());
	}

	private void initBufferSizes() {
		int bufferSize = GlobalConfiguration.getInteger("channel.network.bufferSizeInBytes",
			GlobalBufferPool.DEFAULT_BUFFER_SIZE_IN_BYTES);

		this.maximumBufferSize = bufferSize;

		long now = System.currentTimeMillis();
		for (ProfilingGroupVertex groupVertex : profilingModel.getProfilingSequence().getSequenceVertices()) {
			for (ProfilingVertex vertex : groupVertex.getGroupMembers()) {
				for (ProfilingEdge forwardEdge : vertex.getForwardEdges()) {
					forwardEdge.getEdgeCharacteristics().getBufferSizeHistory().addToHistory(now, bufferSize);
				}
			}
		}
	}

	public long getAdjustmentInterval() {
		return this.adjustmentInterval;
	}

	HashSet<ChannelID> staleEdges = new HashSet<ChannelID>();

	public void adjustBufferSizes(ProfilingSequenceSummary summary) throws InterruptedException {
		HashMap<ProfilingEdge, Integer> edgesToAdjust = new HashMap<ProfilingEdge, Integer>();

		staleEdges.clear();
		for (ProfilingSubsequenceSummary activeSubsequence : summary.enumerateActiveSubsequences()) {
			if (activeSubsequence.getSubsequenceLatency() > latencyGoal) {
				collectEdgesToAdjust(activeSubsequence, edgesToAdjust);
			}
		}

		doAdjust(edgesToAdjust);

		LOG.info(String.format("Adjusted edges: %d | Edges with stale profiling data: %d", edgesToAdjust.size(),
			staleEdges.size()));

		refreshTimeOfNextAdjustment();
	}

	public void logBufferSizes() throws IOException {
		bufferSizeLogger.logBufferSizes();
	}

	private void doAdjust(HashMap<ProfilingEdge, Integer> edgesToAdjust) throws InterruptedException {

		for (ProfilingEdge edge : edgesToAdjust.keySet()) {
			int newBufferSize = edgesToAdjust.get(edge);

			BufferSizeHistory sizeHistory = edge.getEdgeCharacteristics().getBufferSizeHistory();
			sizeHistory.addToHistory(timeOfNextAdjustment, newBufferSize);

			setBufferSize(edge, newBufferSize);
		}
	}

	private void refreshTimeOfNextAdjustment() {
		long now = System.currentTimeMillis();
		while (timeOfNextAdjustment <= now) {
			timeOfNextAdjustment += this.adjustmentInterval;
		}
	}

	private void collectEdgesToAdjust(ProfilingSubsequenceSummary activeSubsequence,
			HashMap<ProfilingEdge, Integer> edgesToAdjust) {

		for (ProfilingEdge edge : activeSubsequence.getEdges()) {

			EdgeCharacteristics edgeChar = edge.getEdgeCharacteristics();

			if (edgesToAdjust.containsKey(edge) || edgeChar.isInChain()) {
				continue;
			}

			if (!hasFreshValues(edgeChar) || !hasFreshValues(edge.getSourceVertex().getVertexLatency())) {
				staleEdges.add(edge.getSourceChannelID());
				// LOG.info("Rejecting edge due to stale values: " + ProfilingUtils.formatName(edge));
				continue;
			}

			// double edgeLatency = edgeChar.getChannelLatencyInMillis();
			double avgOutputBufferLatency = edgeChar.getOutputBufferLifetimeInMillis() / 2;
			double sourceTaskLatency = edge.getSourceVertex().getVertexLatency().getLatencyInMillis();

			// if (avgOutputBufferLatency > 5 && avgOutputBufferLatency >= 0.05 * edgeLatency) {
			if (avgOutputBufferLatency > 5 && avgOutputBufferLatency > sourceTaskLatency) {
				reduceBufferSize(edgeChar, edgesToAdjust);
			} else if (avgOutputBufferLatency <= 1) {
				increaseBufferSize(edgeChar, edgesToAdjust);
			}
		}
	}

	private void increaseBufferSize(EdgeCharacteristics edgeChar, HashMap<ProfilingEdge, Integer> edgesToAdjust) {
		int oldBufferSize = edgeChar.getBufferSizeHistory().getLastEntry().getBufferSize();
		int newBufferSize = Math.min(proposedIncreasedBufferSize(oldBufferSize), this.maximumBufferSize);

		if (isRelevantIncrease(oldBufferSize, newBufferSize)) {
			edgesToAdjust.put(edgeChar.getEdge(), newBufferSize);
		}
	}

	private boolean isRelevantIncrease(int oldBufferSize, int newBufferSize) {
		return newBufferSize >= oldBufferSize + 100;
	}

	private int proposedIncreasedBufferSize(int oldBufferSize) {
		return (int) (oldBufferSize * 1.2);
	}

	private void reduceBufferSize(EdgeCharacteristics edgeChar, HashMap<ProfilingEdge, Integer> edgesToAdjust) {
		int oldBufferSize = edgeChar.getBufferSizeHistory().getLastEntry().getBufferSize();
		int newBufferSize = proposedReducedBufferSize(edgeChar, oldBufferSize);

		// filters pointless minor changes in buffer size
		if (isRelevantReduction(newBufferSize, oldBufferSize)) {
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

	private int proposedReducedBufferSize(EdgeCharacteristics edgeChar, int oldBufferSize) {
		double avgOutputBufferLatency = edgeChar.getOutputBufferLifetimeInMillis() / 2;

		double reductionFactor = Math.pow(0.98, avgOutputBufferLatency);
		reductionFactor = Math.max(0.1, reductionFactor);

		int newBufferSize = (int) Math.max(200, oldBufferSize * reductionFactor);

		return newBufferSize;
	}

	private boolean hasFreshValues(EdgeCharacteristics edgeChar) {
		long freshnessThreshold = edgeChar.getBufferSizeHistory().getLastEntry().getTimestamp();

		return edgeChar.isChannelLatencyFresherThan(freshnessThreshold)
			&& edgeChar.isOutputBufferLatencyFresherThan(freshnessThreshold);
	}

	private boolean hasFreshValues(VertexLatency vertexLatency) {
		return vertexLatency.isActive();
	}

	public boolean isAdjustmentNecessary(long now) {
		return now >= timeOfNextAdjustment;
	}

	private void setBufferSize(ProfilingEdge edge, int bufferSize) throws InterruptedException {
		LimitBufferSizeAction bsla = new LimitBufferSizeAction(jobID, edge.getSourceVertex().getID(),
			edge.getSourceChannelID(), bufferSize);
		this.communicationThread.sendToTaskManagerAsynchronously(edge.getSourceVertex().getProfilingReporter(), bsla);
	}
}
