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

	private OutputChannelStatistics channel12Throughput;

	private OutputChannelStatistics channel13Throughput;

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
		this.channel12Throughput = new OutputChannelStatistics(this.channel12Source,
				21, 13, 22, 23);
		this.channel13Throughput = new OutputChannelStatistics(this.channel13Source,
				31, 11, 33, 34);
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
	public void testAddOutputChannelStatistics() {
		StreamProfilingReport report = new StreamProfilingReport(this.jobID);
		report.addOutputChannelStatistics(this.channel12Throughput);
		report.addOutputChannelStatistics(this.channel13Throughput);
		this.checkOutputChannelStatistics(report);
	}

	private void checkOutputChannelStatistics(StreamProfilingReport report) {
		assertEquals(report.getOutputChannelStatistics().size(), 2);
		Iterator<OutputChannelStatistics> iter = report.getOutputChannelStatistics()
				.iterator();
		OutputChannelStatistics first = iter.next();
		OutputChannelStatistics second = iter.next();

		assertTrue(!first.equals(second));
		assertTrue(first.equals(this.channel12Throughput)
				|| first.equals(this.channel13Throughput));
		assertTrue(second.equals(this.channel12Throughput)
				|| second.equals(this.channel13Throughput));
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
		report.addOutputChannelStatistics(this.channel13Throughput);
		report.addTaskLatency(this.taskLatency3);
		report.addTaskLatency(this.taskLatency1);
		report.addChannelLatency(this.channel13Latency);
		report.addOutputChannelStatistics(this.channel12Throughput);
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
		this.checkOutputChannelStatistics(report);
		this.checkTaskLatencies(report);
	}

	@Test
	public void testReadWriteWhenEmpty() throws IOException {
		StreamProfilingReport report = new StreamProfilingReport(this.jobID);

		ByteArrayOutputStream byteArrayOutStream = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(byteArrayOutStream);
		report.write(out);

		// jobID + false + false + 3 * emptyMap
		int expectedLength = 16 + 1 + 1 + 3 * 4;
		byte[] serializedData = byteArrayOutStream.toByteArray();
		assertEquals(expectedLength, serializedData.length);
		DataInput in = new DataInputStream(new ByteArrayInputStream(
				serializedData));
		report = new StreamProfilingReport();
		report.read(in);
		assertTrue(report.getChannelLatencies().isEmpty());
		assertTrue(report.getOutputChannelStatistics().isEmpty());
		assertTrue(report.getTaskLatencies().isEmpty());
	}

}
