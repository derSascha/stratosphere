package eu.stratosphere.nephele.streaming.profiling.ng;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.profiling.EdgeCharacteristics;
import eu.stratosphere.nephele.streaming.profiling.VertexLatency;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingEdge;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingGroupEdge;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingGroupVertex;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingSequence;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingVertex;

@SuppressWarnings("unused")
public class ProfilingSubsequenceTest {

	private ProfilingVertex vertex00;

	private ProfilingVertex vertex01;

	private ProfilingVertex vertex10;

	private ProfilingVertex vertex11;

	private ProfilingVertex vertex20;

	private ProfilingVertex vertex21;

	private ProfilingVertex vertex30;

	private ProfilingVertex vertex31;

	private ProfilingEdge edge0010;

	private ProfilingEdge edge0011;

	private ProfilingEdge edge0110;

	private ProfilingEdge edge0111;

	private ProfilingEdge edge1020;

	private ProfilingEdge edge1121;

	private ProfilingEdge edge2030;

	private ProfilingEdge edge2031;

	private ProfilingEdge edge2130;

	private ProfilingEdge edge2131;

	private ProfilingGroupVertex groupVertex0;

	private ProfilingGroupVertex groupVertex1;

	private ProfilingGroupVertex groupVertex2;

	private ProfilingGroupVertex groupVertex3;

	private ProfilingGroupEdge groupEdge01;

	private ProfilingGroupEdge groupEdge12;

	private ProfilingGroupEdge groupEdge23;

	private ProfilingSequence sequence;

	private ProfilingVertex vertex32;

	private ProfilingEdge edge2032;

	private ProfilingEdge edge2132;

	@Before
	public void setup() {
		this.groupVertex0 = new ProfilingGroupVertex(new JobVertexID(),
				"group-0");
		this.groupVertex1 = new ProfilingGroupVertex(new JobVertexID(),
				"group-1");
		this.groupVertex2 = new ProfilingGroupVertex(new JobVertexID(),
				"group-2");
		this.groupVertex3 = new ProfilingGroupVertex(new JobVertexID(),
				"group-3");

		this.groupEdge01 = new ProfilingGroupEdge(
				DistributionPattern.BIPARTITE, this.groupVertex0,
				this.groupVertex1);
		this.groupEdge12 = new ProfilingGroupEdge(
				DistributionPattern.POINTWISE, this.groupVertex1,
				this.groupVertex2);
		this.groupEdge23 = new ProfilingGroupEdge(
				DistributionPattern.BIPARTITE, this.groupVertex2,
				this.groupVertex3);

		this.vertex00 = this.createVertex(false, "vertex00", 1);
		this.vertex01 = this.createVertex(false, "vertex01", 2);
		this.vertex10 = this.createVertex(true, "vertex10", 3);
		this.vertex11 = this.createVertex(true, "vertex11", 4);
		this.vertex20 = this.createVertex(true, "vertex20", 5);
		this.vertex21 = this.createVertex(true, "vertex21", 6);
		this.vertex30 = this.createVertex(false, "vertex30", 7);
		this.vertex31 = this.createVertex(false, "vertex31", 8);
		this.vertex32 = this.createVertex(false, "vertex32", 9);

		this.edge0010 = this.connect(this.vertex00, this.vertex10, true, 110);
		this.edge0011 = this.connect(this.vertex00, this.vertex11, true, 120);
		this.edge0110 = this.connect(this.vertex01, this.vertex10, true, 130);
		this.edge0111 = this.connect(this.vertex01, this.vertex11, true, 140);

		this.edge1020 = this.connect(this.vertex10, this.vertex20, true, 210);
		this.edge1121 = this.connect(this.vertex11, this.vertex21, true, 220);

		this.edge2030 = this.connect(this.vertex20, this.vertex30, true, 310);
		this.edge2031 = this.connect(this.vertex20, this.vertex31, true, 320);
		this.edge2032 = this.connect(this.vertex20, this.vertex32, true, 330);
		this.edge2130 = this.connect(this.vertex21, this.vertex30, true, 340);
		this.edge2131 = this.connect(this.vertex21, this.vertex31, true, 350);
		this.edge2132 = this.connect(this.vertex21, this.vertex32, true, 360);

		this.groupVertex0.addGroupMember(this.vertex00);
		this.groupVertex0.addGroupMember(this.vertex01);

		this.groupVertex1.addGroupMember(this.vertex10);
		this.groupVertex1.addGroupMember(this.vertex11);

		this.groupVertex2.addGroupMember(this.vertex20);
		this.groupVertex2.addGroupMember(this.vertex21);

		this.groupVertex3.addGroupMember(this.vertex30);
		this.groupVertex3.addGroupMember(this.vertex31);
		this.groupVertex3.addGroupMember(this.vertex32);

		this.sequence = new ProfilingSequence();
		this.sequence.addSequenceVertex(this.groupVertex0);
		this.sequence.addSequenceVertex(this.groupVertex1);
		this.sequence.addSequenceVertex(this.groupVertex2);
		this.sequence.addSequenceVertex(this.groupVertex3);
		this.sequence.setIncludeStartVertex(false);
		this.sequence.setIncludeEndVertex(false);
	}

	private ProfilingVertex createVertex(final boolean active,
			final String name, final double latency) {
		ProfilingVertex vertex = new ProfilingVertex(new ExecutionVertexID(),
				name);
		VertexLatency vertexLatency = new VertexLatency(vertex) {
			@Override
			public boolean isActive() {
				return active;
			}

			@Override
			public double getLatencyInMillis() {
				return latency;
			}
		};
		vertex.setVertexLatency(vertexLatency);
		return vertex;
	}

	private ProfilingEdge connect(ProfilingVertex from, ProfilingVertex to,
			final boolean active, final long latency) {
		ProfilingEdge edge = new ProfilingEdge(new ChannelID(), new ChannelID());
		from.addForwardEdge(edge);
		to.addBackwardEdge(edge);
		edge.setSourceVertex(from);
		edge.setTargetVertex(to);
		// src/target indices on edge should not play any role

		edge.setEdgeCharacteristics(new EdgeCharacteristics(edge) {
			@Override
			public boolean isActive() {
				return active;
			}

			@Override
			public double getChannelLatencyInMillis() {
				return latency;
			}

			@Override
			public double getOutputBufferLifetimeInMillis() {
				return 40;
			}
		});
		return edge;
	}

	@Test
	public void testConstructorSimple() {
		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(
				this.sequence);
		assertEquals(4, summary.sequenceDepth);

		assertEquals(2, summary.forwardEdgeCounts[0]);
		assertEquals(2, summary.forwardEdgeCounts[1]);
		assertEquals(1, summary.forwardEdgeCounts[2]);
		assertEquals(3, summary.forwardEdgeCounts[3]);

		assertEquals(0, summary.forwardEdgeIndices[0]);
		assertEquals(0, summary.forwardEdgeIndices[1]);
		assertEquals(0, summary.forwardEdgeIndices[2]);
		assertEquals(0, summary.forwardEdgeIndices[3]);

		assertEquals(4, summary.currSubsequence.size());
		assertTrue(summary.currSubsequence.get(0) == this.vertex00);
		assertTrue(summary.currSubsequence.get(1) == this.vertex10);
		assertTrue(summary.currSubsequence.get(2) == this.vertex20);
		assertTrue(summary.currSubsequence.get(3) == this.vertex30);
		assertTrue(summary.isSubsequenceActive());
		assertTrue(summary.getSubsequenceLatency() == 638.0);
		assertTrue(summary.getEdges().size() == 3);
		assertTrue(summary.getEdges().get(0) == this.edge0010);
		assertTrue(summary.getEdges().get(1) == this.edge1020);
		assertTrue(summary.getEdges().get(2) == this.edge2030);
	}

	@Test
	public void testConstructorHarder() {
		this.deactivateEdge(this.edge1020);
		this.deactivateEdge(this.edge0011);
		this.deactivateEdge(this.edge2130);
		this.deactivateEdge(this.edge2131);
		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(
				this.sequence);

		assertEquals(2, summary.forwardEdgeCounts[0]);
		assertEquals(2, summary.forwardEdgeCounts[1]);
		assertEquals(1, summary.forwardEdgeCounts[2]);
		assertEquals(3, summary.forwardEdgeCounts[3]);

		assertEquals(1, summary.forwardEdgeIndices[0]);
		assertEquals(1, summary.forwardEdgeIndices[1]);
		assertEquals(0, summary.forwardEdgeIndices[2]);
		assertEquals(2, summary.forwardEdgeIndices[3]);

		assertEquals(4, summary.currSubsequence.size());
		assertTrue(summary.currSubsequence.get(0) == this.vertex01);
		assertTrue(summary.currSubsequence.get(1) == this.vertex11);
		assertTrue(summary.currSubsequence.get(2) == this.vertex21);
		assertTrue(summary.currSubsequence.get(3) == this.vertex32);
		assertTrue(summary.isSubsequenceActive());
		assertTrue(summary.getSubsequenceLatency() == 730.0);
		assertTrue(summary.getEdges().size() == 3);
		assertTrue(summary.getEdges().get(0) == this.edge0111);
		assertTrue(summary.getEdges().get(1) == this.edge1121);
		assertTrue(summary.getEdges().get(2) == this.edge2132);
	}

	@Test
	public void testConstructorWithNoActiveSequence() {
		this.deactivateEdge(this.edge1020);
		this.deactivateEdge(this.edge0011);
		this.deactivateEdge(this.edge2130);
		this.deactivateEdge(this.edge2131);
		this.deactivateEdge(this.edge2132);
		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(
				this.sequence);

		assertEquals(2, summary.forwardEdgeCounts[0]);
		assertEquals(2, summary.forwardEdgeCounts[1]);
		assertEquals(1, summary.forwardEdgeCounts[2]);
		assertEquals(3, summary.forwardEdgeCounts[3]);
		assertEquals(-1, summary.forwardEdgeIndices[0]);
		assertEquals(-1, summary.forwardEdgeIndices[1]);
		assertEquals(-1, summary.forwardEdgeIndices[2]);
		assertEquals(-1, summary.forwardEdgeIndices[3]);
		assertEquals(0, summary.currSubsequence.size());
		assertFalse(summary.isSubsequenceActive());
	}

	@Test
	public void testSwitchToNextActivePath() {
		this.deactivateEdge(this.edge0011);
		this.deactivateEdge(this.edge2030);
		this.deactivateEdge(this.edge2032);
		this.deactivateEdge(this.edge2130);
		this.deactivateEdge(this.edge2131);
		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(
				this.sequence);

		assertEquals(0, summary.forwardEdgeIndices[0]);
		assertEquals(0, summary.forwardEdgeIndices[1]);
		assertEquals(0, summary.forwardEdgeIndices[2]);
		assertEquals(1, summary.forwardEdgeIndices[3]);
		assertEquals(4, summary.currSubsequence.size());
		assertTrue(summary.isSubsequenceActive());
		assertTrue(summary.currSubsequence.get(0) == this.vertex00);
		assertTrue(summary.currSubsequence.get(1) == this.vertex10);
		assertTrue(summary.currSubsequence.get(2) == this.vertex20);
		assertTrue(summary.currSubsequence.get(3) == this.vertex31);
		assertTrue(summary.getSubsequenceLatency() == 648.0);
		assertTrue(summary.getEdges().size() == 3);
		assertTrue(summary.getEdges().get(0) == this.edge0010);
		assertTrue(summary.getEdges().get(1) == this.edge1020);
		assertTrue(summary.getEdges().get(2) == this.edge2031);

		assertTrue(summary.switchToNextActivePathIfPossible());
		assertEquals(1, summary.forwardEdgeIndices[0]);
		assertEquals(0, summary.forwardEdgeIndices[1]);
		assertEquals(0, summary.forwardEdgeIndices[2]);
		assertEquals(1, summary.forwardEdgeIndices[3]);
		assertEquals(4, summary.currSubsequence.size());
		assertTrue(summary.isSubsequenceActive());
		assertTrue(summary.currSubsequence.get(0) == this.vertex01);
		assertTrue(summary.currSubsequence.get(1) == this.vertex10);
		assertTrue(summary.currSubsequence.get(2) == this.vertex20);
		assertTrue(summary.currSubsequence.get(3) == this.vertex31);
		assertTrue(summary.getSubsequenceLatency() == 668.0);
		assertTrue(summary.getEdges().size() == 3);
		assertTrue(summary.getEdges().get(0) == this.edge0110);
		assertTrue(summary.getEdges().get(1) == this.edge1020);
		assertTrue(summary.getEdges().get(2) == this.edge2031);

		assertTrue(summary.switchToNextActivePathIfPossible());
		assertEquals(1, summary.forwardEdgeIndices[0]);
		assertEquals(1, summary.forwardEdgeIndices[1]);
		assertEquals(0, summary.forwardEdgeIndices[2]);
		assertEquals(2, summary.forwardEdgeIndices[3]);
		assertEquals(4, summary.currSubsequence.size());
		assertTrue(summary.isSubsequenceActive());
		assertTrue(summary.currSubsequence.get(0) == this.vertex01);
		assertTrue(summary.currSubsequence.get(1) == this.vertex11);
		assertTrue(summary.currSubsequence.get(2) == this.vertex21);
		assertTrue(summary.currSubsequence.get(3) == this.vertex32);
		assertTrue(summary.getSubsequenceLatency() == 730.0);
		assertTrue(summary.getEdges().size() == 3);
		assertTrue(summary.getEdges().get(0) == this.edge0111);
		assertTrue(summary.getEdges().get(1) == this.edge1121);
		assertTrue(summary.getEdges().get(2) == this.edge2132);

		assertFalse(summary.switchToNextActivePathIfPossible());
		assertEquals(-1, summary.forwardEdgeIndices[0]);
		assertEquals(-1, summary.forwardEdgeIndices[1]);
		assertEquals(-1, summary.forwardEdgeIndices[2]);
		assertEquals(-1, summary.forwardEdgeIndices[3]);
		assertEquals(0, summary.currSubsequence.size());
		assertFalse(summary.isSubsequenceActive());
	}

	@Test
	public void addCurrentSubsequenceLatenciesTest() {
		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(
				this.sequence);

		double[] aggregatedLats = new double[8];
		summary.addCurrentSubsequenceLatencies(aggregatedLats);

		assertTrue(aggregatedLats[0] == 20);
		assertTrue(aggregatedLats[1] == 90);

		assertTrue(aggregatedLats[2] == 3);

		assertTrue(aggregatedLats[3] == 20);
		assertTrue(aggregatedLats[4] == 190);

		assertTrue(aggregatedLats[5] == 5);

		assertTrue(aggregatedLats[6] == 20);
		assertTrue(aggregatedLats[7] == 290);
	}

	private void deactivateEdge(ProfilingEdge edge) {
		edge.setEdgeCharacteristics(new EdgeCharacteristics(edge) {
			@Override
			public boolean isActive() {
				return false;
			}
		});
	}
}
