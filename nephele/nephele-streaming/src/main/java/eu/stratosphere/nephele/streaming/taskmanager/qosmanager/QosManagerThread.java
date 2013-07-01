package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.AbstractStreamMessage;
import eu.stratosphere.nephele.streaming.message.StreamChainAnnounce;
import eu.stratosphere.nephele.streaming.message.action.ConstructStreamChainAction;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosRolesAction;
import eu.stratosphere.nephele.streaming.message.qosreport.QosReport;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.BufferSizeManager;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;
import eu.stratosphere.nephele.util.StringUtils;

public class QosManagerThread extends Thread {

	private static final Log LOG = LogFactory.getLog(QosManagerThread.class);

	private final LinkedBlockingQueue<AbstractStreamMessage> streamingDataQueue;

	private final StreamMessagingThread messagingThread;

	private ProfilingLogger logger;

	private BufferSizeManager bufferSizeManager;

	private QosModel qosModel;

	private JobID jobID;
	
	private boolean terminated;

	public QosManagerThread(JobID jobID,
			StreamMessagingThread messagingThread) {
		this.jobID = jobID;
		this.messagingThread = messagingThread;
		this.qosModel = new QosModel(jobID);
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractStreamMessage>();

//		this.bufferSizeManager = new BufferSizeManager(this.jobID, 300,
//				this.profilingModel, this.messagingThread);

		try {
			this.logger = new ProfilingLogger(
					this.bufferSizeManager.getAdjustmentInterval());
		} catch (IOException e) {
			LOG.error("Error when opening profiling logger file", e);
		}
	}

	@Override
	public void run() {
		LOG.info("Started profiling master thread.");

		int totalNoOfMessages = 0;
		int channelLats = 0;
		int taskLats = 0;
		int outChannelStats = 0;

		try {
			while (!interrupted()) {
				AbstractStreamMessage streamingData = this.streamingDataQueue
						.take();

				totalNoOfMessages++;

				long now = System.currentTimeMillis();
				if (streamingData instanceof QosReport) {
					this.qosModel.processQosReport((QosReport) streamingData);
				} else if (streamingData instanceof StreamChainAnnounce) {
					// FIXME
//					this.profilingModel
//							.announceStreamingChain((StreamChainAnnounce) streamingData);
				} else if (streamingData instanceof DeployInstanceQosRolesAction) {
					this.qosModel
							.mergeShallowQosGraph(((DeployInstanceQosRolesAction) streamingData)
									.getQosManager().getShallowQosGraph());
				}

				if (this.bufferSizeManager.isAdjustmentNecessary(now)) {
					long beginTime = System.currentTimeMillis();

					ProfilingSequenceSummary summary = this.profilingModel
							.computeProfilingSummary();
					this.bufferSizeManager.adjustBufferSizes(summary);

					try {
						this.logger.logLatencies(summary);
					} catch (IOException e) {
						LOG.error(StringUtils.stringifyException(e));
					}

					long buffersizeAdjustmentOverhead = System
							.currentTimeMillis() - beginTime;
					LOG.info(String
							.format("total messages: %d (channel: %d | task: %d | outChanStats: %d ) || enqueued: %d || buffersizeAdjustmentOverhead: %d",
									totalNoOfMessages, channelLats, taskLats,
									outChannelStats,
									this.streamingDataQueue.size(),
									buffersizeAdjustmentOverhead));

					totalNoOfMessages = 0;
					channelLats = 0;
					taskLats = 0;
					outChannelStats = 0;
				}
			}

		} catch (InterruptedException e) {
		}

		this.cleanUp();
		LOG.info("Stopped profiling master thread");
	}

	private void triggerChainingDelayed(final long delay) {
		final LinkedList<LinkedList<ExecutionVertexID>> chainList = new LinkedList<LinkedList<ExecutionVertexID>>();
		final HashMap<ExecutionVertexID, InstanceConnectionInfo> instances = new HashMap<ExecutionVertexID, InstanceConnectionInfo>();

		for (QosGroupVertex groupVertex : this.profilingSequence
				.getSequenceVertices()) {
			if (groupVertex.getName().startsWith("Decoder")) {

				LOG.info("Decoder group members: "
						+ groupVertex.getMembers().size());
				for (QosVertex decoder : groupVertex.getMembers()) {
					instances.put(decoder.getID(),
							decoder.getExecutingInstance());
					LinkedList<ExecutionVertexID> chain = new LinkedList<ExecutionVertexID>();
					chain.add(decoder.getID());

					QosVertex merger = decoder.getForwardEdges().get(0)
							.getTargetVertex();
					chain.add(merger.getID());

					QosVertex overlay = merger.getForwardEdges().get(0)
							.getTargetVertex();
					chain.add(overlay.getID());

					QosVertex encoder = overlay.getForwardEdges().get(0)
							.getTargetVertex();
					chain.add(encoder.getID());
					chainList.add(chain);
				}
				break;
			}
		}

		LOG.info("Number of chains to announce: " + chainList.size());

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
					QosManagerThread.this
							.handOffStreamingData(new StreamChainAnnounce(
									QosManagerThread.this.jobID, chain
											.getFirst(), chain.getLast()));
				}

				for (LinkedList<ExecutionVertexID> chain : chainList) {
					ConstructStreamChainAction csca = new ConstructStreamChainAction(
							QosManagerThread.this.jobID, chain);
					InstanceConnectionInfo actionReceiver = instances.get(chain
							.getFirst());
					try {
						QosManagerThread.this.messagingThread
								.sendToTaskManagerAsynchronously(
										actionReceiver, csca);
					} catch (InterruptedException e) {
					}
					LOG.info(String.format(
							"Triggered chaining for %d tasks on %s",
							chain.size(), actionReceiver.toString()));
				}
			}
		};
		new Thread(run).start();

	}

	private void cleanUp() {
		this.streamingDataQueue.clear();
		this.qosModel = null;
		this.logger = null;
		this.bufferSizeManager = null;
		// FIXME is this complete?
	}

	public void shutdown() {
		this.interrupt();
	}

	public void handOffStreamingData(AbstractStreamMessage data) {
		this.streamingDataQueue.add(data);
	}
}
