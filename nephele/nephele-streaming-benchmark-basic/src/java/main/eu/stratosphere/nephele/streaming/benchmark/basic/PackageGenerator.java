package eu.stratosphere.nephele.streaming.benchmark.basic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;

/**
 * @author Sascha Wolke
 */
public class PackageGenerator extends AbstractGenericInputTask {
	private static final Log LOG = LogFactory.getLog(PackageGenerator.class);
	private RecordWriter<BasicBenchmarkRecord> packetWriter;
	public static final String USER_TASK_TIMING_COUNT_CONFIG_KEY = "basicBenchmark.packageGenerator.userTaskTimingCount";
	public static final String WARMUP_COUNT_CONFIG_KEY = "basicBenchmark.packageGenerator.warmupCount";
	public static final String WARMUP_WAIT_MS_CONFIG_KEY = "basicBenchmark.packageGenerator.warmupWaitMS";
	public static final String PACKAGE_COUNT_CONFIG_KEY = "basicBenchmark.packageGenerator.packageCount";
	public static final String PACKAGE_SIZE_CONFIG_KEY = "basicBenchmark.packageGenerator.packageSize";
	public static final String DELAY_BETWEEN_PACKEGES_NS_CONFIG_KEY = "basicBenchmark.packageGenerator.delayBetweenPackagesNS";
	
	@Override
	public void registerInputOutput() {
		this.packetWriter = new RecordWriter<BasicBenchmarkRecord>(this, BasicBenchmarkRecord.class);
	}

	@Override
	public void invoke() throws Exception {
		final int userTaskTimingCount = 
				getTaskConfiguration().getInteger(USER_TASK_TIMING_COUNT_CONFIG_KEY, 10);
		final int warmupCount =
				getTaskConfiguration().getInteger(WARMUP_COUNT_CONFIG_KEY, 50);
		final int warmupWaitMS =
				getTaskConfiguration().getInteger(WARMUP_WAIT_MS_CONFIG_KEY, 100);
		final long packageCount = 
				getTaskConfiguration().getLong(PACKAGE_COUNT_CONFIG_KEY, 10000);
		final int packageSize =
				getTaskConfiguration().getInteger(PACKAGE_SIZE_CONFIG_KEY, 26*1024);
		final long delayBetweenPackagesNS =
				getTaskConfiguration().getInteger(DELAY_BETWEEN_PACKEGES_NS_CONFIG_KEY, 50*1000);
		
		LOG.info("Starting package generator with: "
				+ "userTaskTimingCount=" + userTaskTimingCount
				+ ", warmupCount=" + warmupCount
				+ ", warmupWaitMS=" + warmupWaitMS
				+ ", packageCount=" + packageCount
				+ ", packageSize=" + packageSize
				+ ", delayBetweenPackagesNS=" + delayBetweenPackagesNS
				);
		
		BasicBenchmarkRecord benchmarkRecord = new BasicBenchmarkRecord(userTaskTimingCount, packageSize);
		
		for (int i = 0; i < packageCount; i++) {
			if (i == warmupCount)
				Thread.sleep(warmupWaitMS);
			else
				Thread.sleep(delayBetweenPackagesNS / 1000,
						(int)(delayBetweenPackagesNS % 1000));
			
			benchmarkRecord.setPacketIdInStream(i);
			benchmarkRecord.setFirstTiming();
			this.packetWriter.emit(benchmarkRecord);
			
			if (i % 1000 == 0)
				LOG.debug(i + " packages send.");
		}
		
		benchmarkRecord.setEndOfStreamPacket();
		benchmarkRecord.setFirstTiming();
		this.packetWriter.emit(benchmarkRecord);
		this.packetWriter.flush();
		
		// wait 5s until last package arrives
		LOG.info("Generator job done, waiting 5s...");
		Thread.sleep(5*1000);
	}

}
