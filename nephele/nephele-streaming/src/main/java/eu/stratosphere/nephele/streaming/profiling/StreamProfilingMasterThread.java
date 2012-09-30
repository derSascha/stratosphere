package eu.stratosphere.nephele.streaming.profiling;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.StreamingCommunicationThread;
import eu.stratosphere.nephele.streaming.actions.ConstructStreamChainAction;
import eu.stratosphere.nephele.streaming.buffers.BufferSizeManager;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingGroupVertex;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingSequence;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingVertex;
import eu.stratosphere.nephele.streaming.profiling.ng.ProfilingSequenceSummary;
import eu.stratosphere.nephele.streaming.types.AbstractStreamingData;
import eu.stratosphere.nephele.streaming.types.StreamProfilingReport;
import eu.stratosphere.nephele.streaming.types.StreamingChainAnnounce;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.profiling.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.types.profiling.TaskLatency;
import eu.stratosphere.nephele.util.StringUtils;

public class StreamProfilingMasterThread extends Thread {

	private Log LOG = LogFactory.getLog(StreamProfilingMasterThread.class);

	private final LinkedBlockingQueue<AbstractStreamingData> streamingDataQueue;

	private final StreamingCommunicationThread communicationThread;

	private ProfilingLogger logger;

	private BufferSizeManager bufferSizeManager;

	private ProfilingSequence profilingSequence;

	private ProfilingModel profilingModel;

	private JobID jobID;

	public StreamProfilingMasterThread(JobID jobID, StreamingCommunicationThread communicationThread,
			ProfilingSequence profilingSequence) {
		this.jobID = jobID;
		this.communicationThread = communicationThread;
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractStreamingData>();
		this.profilingSequence = profilingSequence;
		this.profilingModel = new ProfilingModel(this.profilingSequence);
		this.bufferSizeManager = new BufferSizeManager(this.jobID, 300, this.profilingModel, this.communicationThread);

		try {
			this.logger = new ProfilingLogger(this.bufferSizeManager.getAdjustmentInterval());
		} catch (IOException e) {
			LOG.error("Error when opening profiling logger file", e);
		}
	}

	public void run() {
		LOG.info("Started profiling master thread.");

		int totalNoOfMessages = 0;
		int channelLats = 0;
		int taskLats = 0;
		int throughputs = 0;
		int obls = 0;

		//triggerChainingDelayed(30000);
		try {
			while (!interrupted()) {
				AbstractStreamingData streamingData = streamingDataQueue.take();

				totalNoOfMessages++;

				long now = System.currentTimeMillis();
				if (streamingData instanceof StreamProfilingReport) {
					StreamProfilingReport profilingReport = (StreamProfilingReport) streamingData;

					for (ChannelLatency channelLatency : profilingReport.getChannelLatencies()) {
						profilingModel.refreshEdgeLatency(now, channelLatency);
						channelLats++;
					}

					for (ChannelThroughput channelThroughput : profilingReport.getChannelThroughputs()) {
						profilingModel.refreshChannelThroughput(now, channelThroughput);
						throughputs++;
					}

					for (TaskLatency taskLatency : profilingReport.getTaskLatencies()) {
						profilingModel.refreshTaskLatency(now, taskLatency);
						taskLats++;
					}

					for (OutputBufferLatency outputBufferLatency : profilingReport.getOutputBufferLatencies()) {
						profilingModel.refreshChannelOutputBufferLatency(now, outputBufferLatency);
						obls++;
					}
				} else if (streamingData instanceof StreamingChainAnnounce) {
					profilingModel.announceStreamingChain((StreamingChainAnnounce) streamingData);
				}

				if (this.bufferSizeManager.isAdjustmentNecessary(now)) {
					long beginTime = System.currentTimeMillis();

					ProfilingSequenceSummary summary = profilingModel.computeProfilingSummary();
					this.bufferSizeManager.adjustBufferSizes(summary);

					try {
						this.logger.logLatencies(summary);
					} catch (IOException e) {
						LOG.error(StringUtils.stringifyException(e));
					}

					long buffersizeAdjustmentOverhead = System.currentTimeMillis() - beginTime;
					LOG.info(String
						.format(
							"total messages: %d (channel: %d | task: %d | throughput: %d | obl: %d) || enqueued: %d || buffersizeAdjustmentOverhead: %d",
							totalNoOfMessages, channelLats, taskLats, throughputs, obls, streamingDataQueue.size(),
							buffersizeAdjustmentOverhead));

					totalNoOfMessages = 0;
					channelLats = 0;
					taskLats = 0;
					throughputs = 0;
					obls = 0;
				}
			}

		} catch (InterruptedException e) {
		}

		cleanUp();
		LOG.info("Stopped profiling master thread");
	}

	private void triggerChainingDelayed(final long delay) {
		final LinkedList<LinkedList<ExecutionVertexID>> chainList = new LinkedList<LinkedList<ExecutionVertexID>>();
		final HashMap<ExecutionVertexID, InstanceConnectionInfo> instances = new HashMap<ExecutionVertexID, InstanceConnectionInfo>();

		for (ProfilingGroupVertex groupVertex : this.profilingSequence.getSequenceVertices()) {
			if (groupVertex.getName().startsWith("Decoder")) {

				for (ProfilingVertex decoder : groupVertex.getGroupMembers()) {
					instances.put(decoder.getID(), decoder.getProfilingReporter());
					LinkedList<ExecutionVertexID> chain = new LinkedList<ExecutionVertexID>();
					chain.add(decoder.getID());

					ProfilingVertex merger = decoder.getForwardEdges().get(0).getTargetVertex();
					chain.add(merger.getID());

					ProfilingVertex overlay = merger.getForwardEdges().get(0).getTargetVertex();
					chain.add(overlay.getID());

					ProfilingVertex encoder = overlay.getForwardEdges().get(0).getTargetVertex();
					chain.add(encoder.getID());
					chainList.add(chain);
				}
				break;
			}
		}

		final Runnable run = new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					e.printStackTrace();
					return;
				}
				for (LinkedList<ExecutionVertexID> chain : chainList) {
					handOffStreamingData(new StreamingChainAnnounce(jobID, chain.getFirst(), chain.getLast()));
				}

				for (LinkedList<ExecutionVertexID> chain : chainList) {
					ConstructStreamChainAction csca = new ConstructStreamChainAction(jobID, chain);
					InstanceConnectionInfo actionReceiver = instances.get(chain.getFirst());
					try {
						communicationThread.sendToTaskManagerAsynchronously(actionReceiver, csca);
					} catch (InterruptedException e) {
					}
					LOG.info(String.format("Triggered chaining for %d tasks on %s", chain.size(),
						actionReceiver.toString()));
				}
			}
		};
		new Thread(run).start();

	}

	private void cleanUp() {
		this.streamingDataQueue.clear();
		this.profilingSequence = null;
		this.logger = null;
		this.profilingSequence = null;
		this.profilingModel = null;
	}

	public void shutdown() {
		this.interrupt();
	}

	public void handOffStreamingData(AbstractStreamingData data) {
		streamingDataQueue.add(data);
	}
}
