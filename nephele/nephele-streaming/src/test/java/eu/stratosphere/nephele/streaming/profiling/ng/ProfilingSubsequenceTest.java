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
		groupVertex0 = new ProfilingGroupVertex(new JobVertexID(), "group-0");
		groupVertex1 = new ProfilingGroupVertex(new JobVertexID(), "group-1");
		groupVertex2 = new ProfilingGroupVertex(new JobVertexID(), "group-2");
		groupVertex3 = new ProfilingGroupVertex(new JobVertexID(), "group-3");

		groupEdge01 = new ProfilingGroupEdge(DistributionPattern.BIPARTITE, groupVertex0, groupVertex1);
		groupEdge12 = new ProfilingGroupEdge(DistributionPattern.POINTWISE, groupVertex1, groupVertex2);
		groupEdge23 = new ProfilingGroupEdge(DistributionPattern.BIPARTITE, groupVertex2, groupVertex3);

		vertex00 = createVertex(false, "vertex00", 1);
		vertex01 = createVertex(false, "vertex01", 2);
		vertex10 = createVertex(true, "vertex10", 3);
		vertex11 = createVertex(true, "vertex11", 4);
		vertex20 = createVertex(true, "vertex20", 5);
		vertex21 = createVertex(true, "vertex21", 6);
		vertex30 = createVertex(false, "vertex30", 7);
		vertex31 = createVertex(false, "vertex31", 8);
		vertex32 = createVertex(false, "vertex32", 9);

		edge0010 = connect(vertex00, vertex10, true, 110);
		edge0011 = connect(vertex00, vertex11, true, 120);
		edge0110 = connect(vertex01, vertex10, true, 130);
		edge0111 = connect(vertex01, vertex11, true, 140);

		edge1020 = connect(vertex10, vertex20, true, 210);
		edge1121 = connect(vertex11, vertex21, true, 220);

		edge2030 = connect(vertex20, vertex30, true, 310);
		edge2031 = connect(vertex20, vertex31, true, 320);
		edge2032 = connect(vertex20, vertex32, true, 330);
		edge2130 = connect(vertex21, vertex30, true, 340);
		edge2131 = connect(vertex21, vertex31, true, 350);
		edge2132 = connect(vertex21, vertex32, true, 360);

		groupVertex0.addGroupMember(vertex00);
		groupVertex0.addGroupMember(vertex01);

		groupVertex1.addGroupMember(vertex10);
		groupVertex1.addGroupMember(vertex11);

		groupVertex2.addGroupMember(vertex20);
		groupVertex2.addGroupMember(vertex21);

		groupVertex3.addGroupMember(vertex30);
		groupVertex3.addGroupMember(vertex31);
		groupVertex3.addGroupMember(vertex32);

		sequence = new ProfilingSequence();
		sequence.addSequenceVertex(groupVertex0);
		sequence.addSequenceVertex(groupVertex1);
		sequence.addSequenceVertex(groupVertex2);
		sequence.addSequenceVertex(groupVertex3);
		sequence.setIncludeStartVertex(false);
		sequence.setIncludeEndVertex(false);
	}

	private ProfilingVertex createVertex(final boolean active, final String name, final double latency) {
		ProfilingVertex vertex = new ProfilingVertex(new ExecutionVertexID()) {
			public String toString() {
				return name;
			}
		};
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

	private ProfilingEdge connect(ProfilingVertex from, ProfilingVertex to, final boolean active, final long latency) {
		ProfilingEdge edge = new ProfilingEdge(new ChannelID(), new ChannelID());
		from.addForwardEdge(edge);
		to.addBackwardEdge(edge);
		edge.setSourceVertex(from);
		edge.setTargetVertex(to);
		// src/target indices on edge should not play any role

		edge.setEdgeCharacteristics(new EdgeCharacteristics(edge) {
			public boolean isActive() {
				return active;
			}

			public double getChannelLatencyInMillis() {
				return latency;
			}
		});
		return edge;
	}

	@Test
	public void testConstructorSimple() {
		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(sequence);
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
		assertTrue(summary.currSubsequence.get(0) == vertex00);
		assertTrue(summary.currSubsequence.get(1) == vertex10);
		assertTrue(summary.currSubsequence.get(2) == vertex20);
		assertTrue(summary.currSubsequence.get(3) == vertex30);
		assertTrue(summary.isSubsequenceActive());
		assertTrue(summary.getSubsequenceLatency() == 638.0);
		assertTrue(summary.getEdgesSortedByLatency().size() == 3);
		assertTrue(summary.getEdgesSortedByLatency().get(0) == edge2030);
		assertTrue(summary.getEdgesSortedByLatency().get(1) == edge1020);
		assertTrue(summary.getEdgesSortedByLatency().get(2) == edge0010);
	}

	@Test
	public void testConstructorHarder() {
		deactivateEdge(edge1020);
		deactivateEdge(edge0011);
		deactivateEdge(edge2130);
		deactivateEdge(edge2131);
		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(sequence);

		assertEquals(2, summary.forwardEdgeCounts[0]);
		assertEquals(2, summary.forwardEdgeCounts[1]);
		assertEquals(1, summary.forwardEdgeCounts[2]);
		assertEquals(3, summary.forwardEdgeCounts[3]);

		assertEquals(1, summary.forwardEdgeIndices[0]);
		assertEquals(1, summary.forwardEdgeIndices[1]);
		assertEquals(0, summary.forwardEdgeIndices[2]);
		assertEquals(2, summary.forwardEdgeIndices[3]);

		assertEquals(4, summary.currSubsequence.size());
		assertTrue(summary.currSubsequence.get(0) == vertex01);
		assertTrue(summary.currSubsequence.get(1) == vertex11);
		assertTrue(summary.currSubsequence.get(2) == vertex21);
		assertTrue(summary.currSubsequence.get(3) == vertex32);
		assertTrue(summary.isSubsequenceActive());
		assertTrue(summary.getSubsequenceLatency() == 730.0);
		assertTrue(summary.getEdgesSortedByLatency().size() == 3);
		assertTrue(summary.getEdgesSortedByLatency().get(0) == edge2132);
		assertTrue(summary.getEdgesSortedByLatency().get(1) == edge1121);
		assertTrue(summary.getEdgesSortedByLatency().get(2) == edge0111);
	}

	@Test
	public void testConstructorWithNoActiveSequence() {
		deactivateEdge(edge1020);
		deactivateEdge(edge0011);
		deactivateEdge(edge2130);
		deactivateEdge(edge2131);
		deactivateEdge(edge2132);
		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(sequence);

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
		deactivateEdge(edge0011);
		deactivateEdge(edge2030);
		deactivateEdge(edge2032);
		deactivateEdge(edge2130);
		deactivateEdge(edge2131);
		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(sequence);

		assertEquals(0, summary.forwardEdgeIndices[0]);
		assertEquals(0, summary.forwardEdgeIndices[1]);
		assertEquals(0, summary.forwardEdgeIndices[2]);
		assertEquals(1, summary.forwardEdgeIndices[3]);
		assertEquals(4, summary.currSubsequence.size());
		assertTrue(summary.isSubsequenceActive());
		assertTrue(summary.currSubsequence.get(0) == vertex00);
		assertTrue(summary.currSubsequence.get(1) == vertex10);
		assertTrue(summary.currSubsequence.get(2) == vertex20);
		assertTrue(summary.currSubsequence.get(3) == vertex31);
		assertTrue(summary.getSubsequenceLatency() == 648.0);
		assertTrue(summary.getEdgesSortedByLatency().size() == 3);
		assertTrue(summary.getEdgesSortedByLatency().get(0) == edge2031);
		assertTrue(summary.getEdgesSortedByLatency().get(1) == edge1020);
		assertTrue(summary.getEdgesSortedByLatency().get(2) == edge0010);


		assertTrue(summary.switchToNextActivePathIfPossible());
		assertEquals(1, summary.forwardEdgeIndices[0]);
		assertEquals(0, summary.forwardEdgeIndices[1]);
		assertEquals(0, summary.forwardEdgeIndices[2]);
		assertEquals(1, summary.forwardEdgeIndices[3]);
		assertEquals(4, summary.currSubsequence.size());
		assertTrue(summary.isSubsequenceActive());
		assertTrue(summary.currSubsequence.get(0) == vertex01);
		assertTrue(summary.currSubsequence.get(1) == vertex10);
		assertTrue(summary.currSubsequence.get(2) == vertex20);
		assertTrue(summary.currSubsequence.get(3) == vertex31);
		assertTrue(summary.getSubsequenceLatency() == 668.0);
		assertTrue(summary.getEdgesSortedByLatency().size() == 3);
		assertTrue(summary.getEdgesSortedByLatency().get(0) == edge2031);
		assertTrue(summary.getEdgesSortedByLatency().get(1) == edge1020);
		assertTrue(summary.getEdgesSortedByLatency().get(2) == edge0110);

		assertTrue(summary.switchToNextActivePathIfPossible());
		assertEquals(1, summary.forwardEdgeIndices[0]);
		assertEquals(1, summary.forwardEdgeIndices[1]);
		assertEquals(0, summary.forwardEdgeIndices[2]);
		assertEquals(2, summary.forwardEdgeIndices[3]);
		assertEquals(4, summary.currSubsequence.size());
		assertTrue(summary.isSubsequenceActive());
		assertTrue(summary.currSubsequence.get(0) == vertex01);
		assertTrue(summary.currSubsequence.get(1) == vertex11);
		assertTrue(summary.currSubsequence.get(2) == vertex21);
		assertTrue(summary.currSubsequence.get(3) == vertex32);
		assertTrue(summary.getSubsequenceLatency() == 730.0);
		assertTrue(summary.getEdgesSortedByLatency().size() == 3);
		assertTrue(summary.getEdgesSortedByLatency().get(0) == edge2132);
		assertTrue(summary.getEdgesSortedByLatency().get(1) == edge1121);
		assertTrue(summary.getEdgesSortedByLatency().get(2) == edge0111);

		assertFalse(summary.switchToNextActivePathIfPossible());
		assertEquals(-1, summary.forwardEdgeIndices[0]);
		assertEquals(-1, summary.forwardEdgeIndices[1]);
		assertEquals(-1, summary.forwardEdgeIndices[2]);
		assertEquals(-1, summary.forwardEdgeIndices[3]);
		assertEquals(0, summary.currSubsequence.size());
		assertFalse(summary.isSubsequenceActive());
	}

	private void deactivateEdge(ProfilingEdge edge) {
		edge.setEdgeCharacteristics(new EdgeCharacteristics(edge) {
			public boolean isActive() {
				return false;
			}
		});
	}
}
