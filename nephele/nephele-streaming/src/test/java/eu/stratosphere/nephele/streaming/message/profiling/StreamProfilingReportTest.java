package eu.stratosphere.nephele.streaming.message.profiling;
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
		this.channel12Latency = new ChannelLatency(this.channel12Target, 13);
		this.channel13Latency = new ChannelLatency(this.channel13Target, 25);
		this.channel12Throughput = new ChannelThroughput(this.channel12Source,
				47);
		this.channel13Throughput = new ChannelThroughput(this.channel13Source,
				5);
		this.obl12 = new OutputBufferLatency(this.channel12Source, 13);
		this.obl13 = new OutputBufferLatency(this.channel13Source, 11);
		this.taskLatency1 = new TaskLatency(this.vertex1, 1);
		this.taskLatency2 = new TaskLatency(this.vertex2, 1);
		this.taskLatency3 = new TaskLatency(this.vertex3, 1);
	}

	@Test
	public void testAddChannelLatency() {
		StreamProfilingReport report = new StreamProfilingReport(this.jobID);
		report.addChannelLatency(this.channel12Latency);
		report.addChannelLatency(this.channel13Latency);
		this.checkChannelLatencies(report);
	}

	private void checkChannelLatencies(StreamProfilingReport report) {
		assertEquals(report.getChannelLatencies().size(), 2);

		Iterator<ChannelLatency> iter = report.getChannelLatencies().iterator();
		ChannelLatency first = iter.next();
		ChannelLatency second = iter.next();

		assertTrue(!first.equals(second));
		assertTrue(first.equals(this.channel12Latency)
				|| first.equals(this.channel13Latency));
		assertTrue(second.equals(this.channel12Latency)
				|| second.equals(this.channel13Latency));
	}

	@Test
	public void testAddChannelThroughput() {
		StreamProfilingReport report = new StreamProfilingReport(this.jobID);
		report.addChannelThroughput(this.channel12Throughput);
		report.addChannelThroughput(this.channel13Throughput);
		this.checkChannelThroughputs(report);
	}

	private void checkChannelThroughputs(StreamProfilingReport report) {
		assertEquals(report.getChannelThroughputs().size(), 2);
		Iterator<ChannelThroughput> iter = report.getChannelThroughputs()
				.iterator();
		ChannelThroughput first = iter.next();
		ChannelThroughput second = iter.next();

		assertTrue(!first.equals(second));
		assertTrue(first.equals(this.channel12Throughput)
				|| first.equals(this.channel13Throughput));
		assertTrue(second.equals(this.channel12Throughput)
				|| second.equals(this.channel13Throughput));
	}

	@Test
	public void testAddOutputBufferLatency() {
		StreamProfilingReport report = new StreamProfilingReport(this.jobID);
		report.addOutputBufferLatency(this.obl12);
		report.addOutputBufferLatency(this.obl13);
		this.checkOutputBufferLatencies(report);
	}

	private void checkOutputBufferLatencies(StreamProfilingReport report) {
		assertEquals(report.getOutputBufferLatencies().size(), 2);
		Iterator<OutputBufferLatency> iter = report.getOutputBufferLatencies()
				.iterator();
		OutputBufferLatency first = iter.next();
		OutputBufferLatency second = iter.next();

		assertTrue(!first.equals(second));
		assertTrue(first.equals(this.obl12) || first.equals(this.obl13));
		assertTrue(second.equals(this.obl12) || second.equals(this.obl13));
	}

	@Test
	public void testAddTaskLatency() {
		StreamProfilingReport report = new StreamProfilingReport(this.jobID);
		report.addTaskLatency(this.taskLatency1);
		report.addTaskLatency(this.taskLatency2);
		report.addTaskLatency(this.taskLatency3);
		this.checkTaskLatencies(report);
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
		assertTrue(first.equals(this.taskLatency1)
				|| first.equals(this.taskLatency2)
				|| first.equals(this.taskLatency3));
		assertTrue(second.equals(this.taskLatency1)
				|| second.equals(this.taskLatency2)
				|| second.equals(this.taskLatency3));
		assertTrue(third.equals(this.taskLatency1)
				|| third.equals(this.taskLatency2)
				|| third.equals(this.taskLatency3));
	}

	@Test
	public void testReadWrite() throws IOException {
		StreamProfilingReport report = new StreamProfilingReport(this.jobID);
		report.addChannelThroughput(this.channel13Throughput);
		report.addTaskLatency(this.taskLatency3);
		report.addOutputBufferLatency(this.obl12);
		report.addTaskLatency(this.taskLatency1);
		report.addChannelLatency(this.channel13Latency);
		report.addOutputBufferLatency(this.obl13);
		report.addChannelThroughput(this.channel12Throughput);
		report.addTaskLatency(this.taskLatency2);
		report.addChannelLatency(this.channel12Latency);

		ByteArrayOutputStream byteArrayOutStream = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(byteArrayOutStream);
		report.write(out);

		DataInput in = new DataInputStream(new ByteArrayInputStream(
				byteArrayOutStream.toByteArray()));
		report = new StreamProfilingReport();
		report.read(in);
		this.checkChannelLatencies(report);
		this.checkChannelThroughputs(report);
		this.checkOutputBufferLatencies(report);
		this.checkTaskLatencies(report);
	}

	@Test
	public void testReadWriteWhenEmpty() throws IOException {
		StreamProfilingReport report = new StreamProfilingReport(this.jobID);

		ByteArrayOutputStream byteArrayOutStream = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(byteArrayOutStream);
		report.write(out);

		// jobID + false + false + 4 * emptyMap
		int expectedLength = 16 + 1 + 1 + 4 * 4;
		byte[] serializedData = byteArrayOutStream.toByteArray();
		assertEquals(expectedLength, serializedData.length);
		DataInput in = new DataInputStream(new ByteArrayInputStream(
				serializedData));
		report = new StreamProfilingReport();
		report.read(in);
		assertTrue(report.getChannelLatencies().isEmpty());
		assertTrue(report.getChannelThroughputs().isEmpty());
		assertTrue(report.getOutputBufferLatencies().isEmpty());
		assertTrue(report.getTaskLatencies().isEmpty());
	}

}
