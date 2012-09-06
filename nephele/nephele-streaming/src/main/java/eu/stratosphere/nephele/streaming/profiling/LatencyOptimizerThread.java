package eu.stratosphere.nephele.streaming.profiling;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.streaming.StreamingJobManagerPlugin;
import eu.stratosphere.nephele.streaming.buffers.BufferSizeManager;
import eu.stratosphere.nephele.streaming.types.AbstractStreamingData;
import eu.stratosphere.nephele.streaming.types.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.types.StreamingChainAnnounce;
import eu.stratosphere.nephele.streaming.types.TaskLatency;

public class LatencyOptimizerThread extends Thread {

	private Log LOG = LogFactory.getLog(LatencyOptimizerThread.class);

	private final LinkedBlockingQueue<AbstractStreamingData> streamingDataQueue;

	private final StreamingJobManagerPlugin jobManagerPlugin;

	private final ExecutionGraph executionGraph;

	private final ProfilingModel profilingModel;

	private ProfilingLogger logger;

	private BufferSizeManager bufferSizeManager;

	public LatencyOptimizerThread(StreamingJobManagerPlugin jobManagerPlugin, ExecutionGraph executionGraph) {
		this.jobManagerPlugin = jobManagerPlugin;
		this.executionGraph = executionGraph;
		this.profilingModel = new ProfilingModel(executionGraph);
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractStreamingData>();
		try {
			this.logger = new ProfilingLogger();
		} catch (IOException e) {
			LOG.error("Error when opening profiling logger file", e);
		}

		try {
			this.bufferSizeManager = new BufferSizeManager(200, this.profilingModel, this.jobManagerPlugin,
				this.executionGraph);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	public void run() {
		LOG.info("Started optimizer thread for job " + executionGraph.getJobName());

		int totalNoOfMessages = 0;
		int channelLats = 0;
		int taskLats = 0;
		int throughputs = 0;
		int obls = 0;
		
		
		try {
			while (!interrupted()) {
				AbstractStreamingData streamingData = streamingDataQueue.take();
				
				totalNoOfMessages++;

				long now = System.currentTimeMillis();
				if (streamingData instanceof ChannelLatency) {
					profilingModel.refreshEdgeLatency(now, (ChannelLatency) streamingData);
					channelLats++;
				} else if (streamingData instanceof TaskLatency) {
					profilingModel.refreshTaskLatency(now, (TaskLatency) streamingData);
					taskLats++;
				} else if (streamingData instanceof ChannelThroughput) {
					profilingModel.refreshChannelThroughput(now, (ChannelThroughput) streamingData);
					throughputs++;
				} else if (streamingData instanceof OutputBufferLatency) {
					profilingModel.refreshChannelOutputBufferLatency(now, (OutputBufferLatency) streamingData);
					obls++;
				} else if (streamingData instanceof StreamingChainAnnounce) {
					profilingModel.announceStreamingChain((StreamingChainAnnounce) streamingData);
				}

				if (this.logger.isLoggingNecessary(now)) {
					ProfilingSummary summary = profilingModel.computeProfilingSummary();
					try {
						logger.logLatencies(summary);
					} catch (IOException e) {
						LOG.error("Error when writing to profiling logger file", e);
					}

					if (bufferSizeManager.isAdjustmentNecessary(now)) {
						bufferSizeManager.adjustBufferSizes(summary);
						try {
							bufferSizeManager.logBufferSizes();
						} catch (IOException e) {
							LOG.error(e.getMessage(), e);
						}
					}
					
//					LOG.info(String.format("total messages: %d (channel: %d | task: %d | throughput: %d | obl: %d) enqueued: %d\n", 
//						totalNoOfMessages, channelLats, taskLats, throughputs, obls, streamingDataQueue.size()));
					
					totalNoOfMessages = 0;
					channelLats = 0;
					taskLats = 0;
					throughputs = 0;
					obls = 0;
				}
			}

		} catch (InterruptedException e) {
		}

		LOG.info("Stopped optimizer thread for job " + executionGraph.getJobName());
	}

	public void handOffStreamingData(AbstractStreamingData data) {
		streamingDataQueue.add(data);
	}

}
