package eu.stratosphere.nephele.streaming.profiling;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.streaming.StreamingCommunicationThread;
import eu.stratosphere.nephele.streaming.buffers.BufferSizeManager;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingSequence;
import eu.stratosphere.nephele.streaming.types.AbstractStreamingData;
import eu.stratosphere.nephele.streaming.types.StreamProfilingReport;
import eu.stratosphere.nephele.streaming.types.StreamingChainAnnounce;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.profiling.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.types.profiling.TaskLatency;

public class JobStreamProfilingMasterThread extends Thread {

	private Log LOG = LogFactory.getLog(JobStreamProfilingMasterThread.class);

	private final LinkedBlockingQueue<AbstractStreamingData> streamingDataQueue;

	private ProfilingLogger logger;

	private BufferSizeManager bufferSizeManager;

	private ProfilingSequence profilingSequence;

	public JobStreamProfilingMasterThread(StreamingCommunicationThread communicationThread,
			ProfilingSequence profilingSequence) {
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractStreamingData>();
		this.profilingSequence = profilingSequence;

		try {
			this.logger = new ProfilingLogger();
		} catch (IOException e) {
			LOG.error("Error when opening profiling logger file", e);
		}

		// try {
		// this.bufferSizeManager = new BufferSizeManager(200, this.profilingModel, this.jobManagerPlugin,
		// this.executionGraph);
		// } catch (IOException e) {
		// LOG.error(e.getMessage(), e);
		// }
	}

	public void run() {
		LOG.info("Started profiling master thread.");

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
				if (streamingData instanceof StreamProfilingReport) {
					StreamProfilingReport profilingReport = (StreamProfilingReport) streamingData;

					for (ChannelLatency channelLatency : profilingReport.getChannelLatencies()) {
						// profilingModel.refreshEdgeLatency(now, channelLatency);
						channelLats++;
					}

					for (ChannelThroughput channelThroughput : profilingReport.getChannelThroughputs()) {
						// profilingModel.refreshChannelThroughput(now, channelThroughput);
						throughputs++;
					}

					for (TaskLatency taskLatency : profilingReport.getTaskLatencies()) {
						// profilingModel.refreshTaskLatency(now, taskLatency);
						taskLats++;
					}

					for (OutputBufferLatency outputBufferLatency : profilingReport.getOutputBufferLatencies()) {
						// profilingModel.refreshChannelOutputBufferLatency(now, outputBufferLatency);
						obls++;
					}
				} else if (streamingData instanceof StreamingChainAnnounce) {
					// profilingModel.announceStreamingChain((StreamingChainAnnounce) streamingData);
				}

				if (this.logger.isLoggingNecessary(now)) {
					// ProfilingSummary summary = profilingModel.computeProfilingSummary();
					// try {
					// logger.logLatencies(summary);
					// } catch (IOException e) {
					// LOG.error("Error when writing to profiling logger file", e);
					// }
					//
					// if (bufferSizeManager.isAdjustmentNecessary(now)) {
					// bufferSizeManager.adjustBufferSizes(summary);
					// try {
					// bufferSizeManager.logBufferSizes();
					// } catch (IOException e) {
					// LOG.error(e.getMessage(), e);
					// }
					// }

					// FIXME remove me
					this.logger.refreshTimeOfNextLogging();

					LOG.info(String.format(
						"total messages: %d (channel: %d | task: %d | throughput: %d | obl: %d) enqueued: %d\n",
						totalNoOfMessages, channelLats, taskLats, throughputs, obls, streamingDataQueue.size()));

					totalNoOfMessages = 0;
					channelLats = 0;
					taskLats = 0;
					throughputs = 0;
					obls = 0;
				}
			}

		} catch (InterruptedException e) {
		}

		LOG.info("Stopped profiling master thread");
	}

	public void handOffStreamingData(AbstractStreamingData data) {
		streamingDataQueue.add(data);
	}

}
