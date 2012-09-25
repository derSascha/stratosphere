import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.types.StreamProfilingReport;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.profiling.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.profiling.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.types.profiling.TaskLatency;

public class StreamProfilingReportTest {

	private JobID jobID;

	private ExecutionVertexID vertex1;

	private ExecutionVertexID vertex2;

	private ExecutionVertexID vertex3;

	private ChannelID channel12Source;

	private ChannelID channel13Source;

	private ChannelID channel12Target;

	private ChannelID channel13Target;

	private ChannelLatency channel12Latency;

	private ChannelLatency channel13Latency;

	private ChannelThroughput channel12Throughput;

	private ChannelThroughput channel13Throughput;

	private OutputBufferLatency obl12;

	private OutputBufferLatency obl13;

	private TaskLatency taskLatency1;

	private TaskLatency taskLatency2;

	private TaskLatency taskLatency3;

	@Before
	public void setup() {
		this.jobID = new JobID();
		this.vertex1 = new ExecutionVertexID();
		this.vertex2 = new ExecutionVertexID();
		this.vertex3 = new ExecutionVertexID();
		this.channel12Source = new ChannelID();
		this.channel13Source = new ChannelID();
		this.channel12Target = new ChannelID();
		this.channel13Target = new ChannelID();
		this.channel12Latency = new ChannelLatency(channel12Target, 13);
		this.channel13Latency = new ChannelLatency(channel13Target, 25);
		this.channel12Throughput = new ChannelThroughput(channel12Source, 47);
		this.channel13Throughput = new ChannelThroughput(channel13Source, 5);
		this.obl12 = new OutputBufferLatency(channel12Source, 13);
		this.obl13 = new OutputBufferLatency(channel13Source, 11);
		this.taskLatency1 = new TaskLatency(vertex1, 1);
		this.taskLatency2 = new TaskLatency(vertex2, 1);
		this.taskLatency3 = new TaskLatency(vertex3, 1);
	}

	@Test
	public void testAddChannelLatency() {
		StreamProfilingReport report = new StreamProfilingReport(jobID);
		report.addChannelLatency(channel12Latency);
		report.addChannelLatency(channel13Latency);
		checkChannelLatencies(report);
	}

	private void checkChannelLatencies(StreamProfilingReport report) {
		assertEquals(report.getChannelLatencies().size(), 2);

		Iterator<ChannelLatency> iter = report.getChannelLatencies().iterator();
		ChannelLatency first = iter.next();
		ChannelLatency second = iter.next();

		assertTrue(!first.equals(second));
		assertTrue(first.equals(channel12Latency) || first.equals(channel13Latency));
		assertTrue(second.equals(channel12Latency) || second.equals(channel13Latency));
	}

	@Test
	public void testAddChannelThroughput() {
		StreamProfilingReport report = new StreamProfilingReport(jobID);
		report.addChannelThroughput(channel12Throughput);
		report.addChannelThroughput(channel13Throughput);
		checkChannelThroughputs(report);
	}

	private void checkChannelThroughputs(StreamProfilingReport report) {
		assertEquals(report.getChannelThroughputs().size(), 2);
		Iterator<ChannelThroughput> iter = report.getChannelThroughputs().iterator();
		ChannelThroughput first = iter.next();
		ChannelThroughput second = iter.next();

		assertTrue(!first.equals(second));
		assertTrue(first.equals(channel12Throughput) || first.equals(channel13Throughput));
		assertTrue(second.equals(channel12Throughput) || second.equals(channel13Throughput));
	}

	@Test
	public void testAddOutputBufferLatency() {
		StreamProfilingReport report = new StreamProfilingReport(jobID);
		report.addOutputBufferLatency(obl12);
		report.addOutputBufferLatency(obl13);
		checkOutputBufferLatencies(report);
	}

	private void checkOutputBufferLatencies(StreamProfilingReport report) {
		assertEquals(report.getOutputBufferLatencies().size(), 2);
		Iterator<OutputBufferLatency> iter = report.getOutputBufferLatencies().iterator();
		OutputBufferLatency first = iter.next();
		OutputBufferLatency second = iter.next();

		assertTrue(!first.equals(second));
		assertTrue(first.equals(obl12) || first.equals(obl13));
		assertTrue(second.equals(obl12) || second.equals(obl13));
	}

	@Test
	public void testAddTaskLatency() {
		StreamProfilingReport report = new StreamProfilingReport(jobID);
		report.addTaskLatency(taskLatency1);
		report.addTaskLatency(taskLatency2);
		report.addTaskLatency(taskLatency3);
		checkTaskLatencies(report);
	}

	private void checkTaskLatencies(StreamProfilingReport report) {
		assertEquals(report.getTaskLatencies().size(), 3);
		Iterator<TaskLatency> iter = report.getTaskLatencies().iterator();
		TaskLatency first = iter.next();
		TaskLatency second = iter.next();
		TaskLatency third = iter.next();

		assertTrue(!first.equals(second));
		assertTrue(!second.equals(third));
		assertTrue(!first.equals(third));
		assertTrue(first.equals(taskLatency1) || first.equals(taskLatency2) || first.equals(taskLatency3));
		assertTrue(second.equals(taskLatency1) || second.equals(taskLatency2) || second.equals(taskLatency3));
		assertTrue(third.equals(taskLatency1) || third.equals(taskLatency2) || third.equals(taskLatency3));
	}

	@Test
	public void testReadWrite() throws IOException {
		StreamProfilingReport report = new StreamProfilingReport(jobID);
		report.addChannelThroughput(channel13Throughput);
		report.addTaskLatency(taskLatency3);
		report.addOutputBufferLatency(obl12);
		report.addTaskLatency(taskLatency1);
		report.addChannelLatency(channel13Latency);
		report.addOutputBufferLatency(obl13);
		report.addChannelThroughput(channel12Throughput);
		report.addTaskLatency(taskLatency2);
		report.addChannelLatency(channel12Latency);

		ByteArrayOutputStream byteArrayOutStream = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(byteArrayOutStream);
		report.write(out);

		DataInput in = new DataInputStream(new ByteArrayInputStream(byteArrayOutStream.toByteArray()));
		report = new StreamProfilingReport();
		report.read(in);
		checkChannelLatencies(report);
		checkChannelThroughputs(report);
		checkOutputBufferLatencies(report);
		checkTaskLatencies(report);
	}
	
	@Test
	public void testReadWriteWhenEmpty() throws IOException {
		StreamProfilingReport report = new StreamProfilingReport(jobID);

		ByteArrayOutputStream byteArrayOutStream = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(byteArrayOutStream);
		report.write(out);

		// jobID + false + false + 4 * emptyMap
		int expectedLength = 16  + 1 + 1 + (4*4);
		byte[] serializedData = byteArrayOutStream.toByteArray();
		assertEquals(expectedLength, serializedData.length);
		DataInput in = new DataInputStream(new ByteArrayInputStream(serializedData));
		report = new StreamProfilingReport();
		report.read(in);
		assertTrue(report.getChannelLatencies().isEmpty());
		assertTrue(report.getChannelThroughputs().isEmpty());
		assertTrue(report.getOutputBufferLatencies().isEmpty());
		assertTrue(report.getTaskLatencies().isEmpty());
	}

}
