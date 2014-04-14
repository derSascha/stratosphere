package eu.stratosphere.nephele.streaming.benchmark.basic;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;

/**
 * This tasks adds a user task timing to incoming
 * records and immediately emits them.
 * 
 * @author Sascha Wolke
 */
public class FowardTask extends AbstractTask {
	private RecordReader<BasicBenchmarkRecord> packetReader;
	private RecordWriter<BasicBenchmarkRecord> packetWriter;

	@Override
	public void registerInputOutput() {
		this.packetReader = new RecordReader<BasicBenchmarkRecord>(this, BasicBenchmarkRecord.class);
		this.packetWriter = new RecordWriter<BasicBenchmarkRecord>(this, BasicBenchmarkRecord.class);
	}

	@Override
	public void invoke() throws Exception {
		while(this.packetReader.hasNext()) {
			BasicBenchmarkRecord record = this.packetReader.next();
			
			record.addUserTaskTiming();
			
			this.packetWriter.emit(record);
			
			if(record.isEndOfStreamPacket())
				this.packetWriter.flush();
		}

	}
}
