package eu.stratosphere.nephele.streaming.benchmark.basic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;

/**
 * @author Sascha Wolke
 */
public class PackageCollector extends AbstractFileOutputTask {
	private RecordReader<BasicBenchmarkRecord> recordReader;
	private int PACKAGE_COUNT_BETWEEN_REPORTS = 100;

	@Override
	public void registerInputOutput() {
		this.recordReader = new RecordReader<BasicBenchmarkRecord>(this, BasicBenchmarkRecord.class);
	}

	@Override
	public void invoke() throws Exception {
		final int userTaskTimingCount = 
				getTaskConfiguration().getInteger(PackageGenerator.USER_TASK_TIMING_COUNT_CONFIG_KEY, 10);
		long timings[][] = new long[userTaskTimingCount - 1][PACKAGE_COUNT_BETWEEN_REPORTS];
		int timingCount = 0;
		BufferedWriter statisticsWriter = new BufferedWriter(new FileWriter(new File(getFileOutputPath().toUri())));

		while(this.recordReader.hasNext()) {
			BasicBenchmarkRecord benchmarkRecord = this.recordReader.next();
			benchmarkRecord.addUserTaskTiming();
			
			if(benchmarkRecord.isEndOfStreamPacket()) {
				
			} else {
				for (int i = 1; i < userTaskTimingCount; i++)
					timings[i - 1][timingCount] = benchmarkRecord.getUserTaskTimings()[i] - benchmarkRecord.getUserTaskTimings()[0];
				timingCount++;
				
				if (timingCount % PACKAGE_COUNT_BETWEEN_REPORTS == 0) {
					dumpTimings(statisticsWriter, timings);
					timings = new long[userTaskTimingCount - 1][PACKAGE_COUNT_BETWEEN_REPORTS];
					timingCount = 0;
				}
			}
		}
		
		statisticsWriter.close();
	}

	private void dumpTimings(BufferedWriter ouputWriter, long[][] timings) throws IOException {
		int timingFields = timings.length;
		long min[] = new long[timingFields];
		long max[] = new long[timingFields];
		long avg[] = new long[timingFields];
		long median[] = new long[timingFields];
		StringBuilder sb = new StringBuilder();		
		
		for (int i = 0; i < timingFields; i++) {
			Arrays.sort(timings[i]);
			min[i] = timings[i][0];
			max[i] = timings[i][timings[i].length - 1];
			
			avg[i] = 0;
			for (int j = 0; j < timings[i].length; j++)
				avg[i] += timings[i][j];
			avg[i] /= timings[i].length;
			
			if (timings[i].length % 2 == 0)
				median[i] = (timings[i][(timings[i].length/2) - 1] + timings[i][timings[i].length/2]) / 2;
			else
				median[i] = timings[i][timings[i].length/2];
		}
		
		sb.append("{\n\t\"timestamp\": ");
		sb.append(System.currentTimeMillis());
		sb.append(",\n\t\"min\": [");
		sb.append(Arrays.toString(min));
		sb.append("],\n\t\"max\": [");
		sb.append(Arrays.toString(max));
		sb.append("],\n\t\"avg\": [");
		sb.append(Arrays.toString(avg));
		sb.append("],\n\t\"median\": [");
		sb.append(Arrays.toString(median));
		sb.append("]\n},\n");
		
		ouputWriter.write(sb.toString());
		ouputWriter.flush();
	}
}
