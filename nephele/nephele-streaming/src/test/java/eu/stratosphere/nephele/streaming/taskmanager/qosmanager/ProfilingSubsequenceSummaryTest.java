package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.EdgeQosData;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingSequence;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.VertexQosData;

@SuppressWarnings("unused")
public class ProfilingSubsequenceSummaryTest {
	
	//FIXME

//	private QosVertex vertex00;
//
//	private QosVertex vertex01;
//
//	private QosVertex vertex10;
//
//	private QosVertex vertex11;
//
//	private QosVertex vertex20;
//
//	private QosVertex vertex21;
//
//	private QosVertex vertex30;
//
//	private QosVertex vertex31;
//
//	private QosEdge edge0010;
//
//	private QosEdge edge0011;
//
//	private QosEdge edge0110;
//
//	private QosEdge edge0111;
//
//	private QosEdge edge1020;
//
//	private QosEdge edge1121;
//
//	private QosEdge edge2030;
//
//	private QosEdge edge2031;
//
//	private QosEdge edge2130;
//
//	private QosEdge edge2131;
//
//	private QosGroupVertex groupVertex0;
//
//	private QosGroupVertex groupVertex1;
//
//	private QosGroupVertex groupVertex2;
//
//	private QosGroupVertex groupVertex3;
//
//	private QosGroupEdge groupEdge01;
//
//	private QosGroupEdge groupEdge12;
//
//	private QosGroupEdge groupEdge23;
//
//	private ProfilingSequence sequence;
//
//	private QosVertex vertex32;
//
//	private QosEdge edge2032;
//
//	private QosEdge edge2132;
//
//	@Before
//	public void setup() {
//		this.groupVertex0 = new QosGroupVertex(new JobVertexID(),
//				"group-0");
//		this.groupVertex1 = new QosGroupVertex(new JobVertexID(),
//				"group-1");
//		this.groupVertex2 = new QosGroupVertex(new JobVertexID(),
//				"group-2");
//		this.groupVertex3 = new QosGroupVertex(new JobVertexID(),
//				"group-3");
//
//		this.groupEdge01 = new QosGroupEdge(
//				DistributionPattern.BIPARTITE, this.groupVertex0,
//				this.groupVertex1);
//		this.groupEdge12 = new QosGroupEdge(
//				DistributionPattern.POINTWISE, this.groupVertex1,
//				this.groupVertex2);
//		this.groupEdge23 = new QosGroupEdge(
//				DistributionPattern.BIPARTITE, this.groupVertex2,
//				this.groupVertex3);
//
//		this.vertex00 = this.createVertex(false, "vertex00", 1);
//		this.vertex01 = this.createVertex(false, "vertex01", 2);
//		this.vertex10 = this.createVertex(true, "vertex10", 3);
//		this.vertex11 = this.createVertex(true, "vertex11", 4);
//		this.vertex20 = this.createVertex(true, "vertex20", 5);
//		this.vertex21 = this.createVertex(true, "vertex21", 6);
//		this.vertex30 = this.createVertex(false, "vertex30", 7);
//		this.vertex31 = this.createVertex(false, "vertex31", 8);
//		this.vertex32 = this.createVertex(false, "vertex32", 9);
//
//		this.edge0010 = this.connect(this.vertex00, this.vertex10, true, 110);
//		this.edge0011 = this.connect(this.vertex00, this.vertex11, true, 120);
//		this.edge0110 = this.connect(this.vertex01, this.vertex10, true, 130);
//		this.edge0111 = this.connect(this.vertex01, this.vertex11, true, 140);
//
//		this.edge1020 = this.connect(this.vertex10, this.vertex20, true, 210);
//		this.edge1121 = this.connect(this.vertex11, this.vertex21, true, 220);
//
//		this.edge2030 = this.connect(this.vertex20, this.vertex30, true, 310);
//		this.edge2031 = this.connect(this.vertex20, this.vertex31, true, 320);
//		this.edge2032 = this.connect(this.vertex20, this.vertex32, true, 330);
//		this.edge2130 = this.connect(this.vertex21, this.vertex30, true, 340);
//		this.edge2131 = this.connect(this.vertex21, this.vertex31, true, 350);
//		this.edge2132 = this.connect(this.vertex21, this.vertex32, true, 360);
//
//		this.groupVertex0.addGroupMember(this.vertex00);
//		this.groupVertex0.addGroupMember(this.vertex01);
//
//		this.groupVertex1.addGroupMember(this.vertex10);
//		this.groupVertex1.addGroupMember(this.vertex11);
//
//		this.groupVertex2.addGroupMember(this.vertex20);
//		this.groupVertex2.addGroupMember(this.vertex21);
//
//		this.groupVertex3.addGroupMember(this.vertex30);
//		this.groupVertex3.addGroupMember(this.vertex31);
//		this.groupVertex3.addGroupMember(this.vertex32);
//
//		this.sequence = new ProfilingSequence();
//		this.sequence.addSequenceVertex(this.groupVertex0);
//		this.sequence.addSequenceVertex(this.groupVertex1);
//		this.sequence.addSequenceVertex(this.groupVertex2);
//		this.sequence.addSequenceVertex(this.groupVertex3);
//		this.sequence.setIncludeStartVertex(false);
//		this.sequence.setIncludeEndVertex(false);
//	}
//
//	private QosVertex createVertex(final boolean active,
//			final String name, final double latency) {
//		QosVertex vertex = new QosVertex(new ExecutionVertexID(),
//				name);
//		VertexQosData vertexLatency = new VertexQosData(vertex) {
//			@Override
//			public boolean isActive() {
//				return active;
//			}
//
//			@Override
//			public double getLatencyInMillis() {
//				return latency;
//			}
//		};
//		vertex.setQosData(vertexLatency);
//		return vertex;
//	}
//
//	private QosEdge connect(QosVertex from, QosVertex to,
//			final boolean active, final long latency) {
//		QosEdge edge = new QosEdge(new ChannelID(), new ChannelID());
//		from.addForwardEdge(edge);
//		to.addBackwardEdge(edge);
//		edge.setSourceVertex(from);
//		edge.setTargetVertex(to);
//		// src/target indices on edge should not play any role
//
//		edge.setQosData(new EdgeQosData(edge) {
//			@Override
//			public boolean isActive() {
//				return active;
//			}
//
//			@Override
//			public double getChannelLatencyInMillis() {
//				return latency;
//			}
//
//			@Override
//			public double getOutputBufferLifetimeInMillis() {
//				return 40;
//			}
//		});
//		return edge;
//	}
//
//	@Test
//	public void testConstructorSimple() {
//		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(
//				this.sequence);
//		assertEquals(4, summary.sequenceDepth);
//
//		assertEquals(2, summary.forwardEdgeCounts[0]);
//		assertEquals(2, summary.forwardEdgeCounts[1]);
//		assertEquals(1, summary.forwardEdgeCounts[2]);
//		assertEquals(3, summary.forwardEdgeCounts[3]);
//
//		assertEquals(0, summary.forwardEdgeIndices[0]);
//		assertEquals(0, summary.forwardEdgeIndices[1]);
//		assertEquals(0, summary.forwardEdgeIndices[2]);
//		assertEquals(0, summary.forwardEdgeIndices[3]);
//
//		assertEquals(4, summary.currSubsequence.size());
//		assertTrue(summary.currSubsequence.get(0) == this.vertex00);
//		assertTrue(summary.currSubsequence.get(1) == this.vertex10);
//		assertTrue(summary.currSubsequence.get(2) == this.vertex20);
//		assertTrue(summary.currSubsequence.get(3) == this.vertex30);
//		assertTrue(summary.isSubsequenceActive());
//		assertTrue(summary.getSubsequenceLatency() == 638.0);
//		assertTrue(summary.getEdges().size() == 3);
//		assertTrue(summary.getEdges().get(0) == this.edge0010);
//		assertTrue(summary.getEdges().get(1) == this.edge1020);
//		assertTrue(summary.getEdges().get(2) == this.edge2030);
//	}
//
//	@Test
//	public void testConstructorHarder() {
//		this.deactivateEdge(this.edge1020);
//		this.deactivateEdge(this.edge0011);
//		this.deactivateEdge(this.edge2130);
//		this.deactivateEdge(this.edge2131);
//		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(
//				this.sequence);
//
//		assertEquals(2, summary.forwardEdgeCounts[0]);
//		assertEquals(2, summary.forwardEdgeCounts[1]);
//		assertEquals(1, summary.forwardEdgeCounts[2]);
//		assertEquals(3, summary.forwardEdgeCounts[3]);
//
//		assertEquals(1, summary.forwardEdgeIndices[0]);
//		assertEquals(1, summary.forwardEdgeIndices[1]);
//		assertEquals(0, summary.forwardEdgeIndices[2]);
//		assertEquals(2, summary.forwardEdgeIndices[3]);
//
//		assertEquals(4, summary.currSubsequence.size());
//		assertTrue(summary.currSubsequence.get(0) == this.vertex01);
//		assertTrue(summary.currSubsequence.get(1) == this.vertex11);
//		assertTrue(summary.currSubsequence.get(2) == this.vertex21);
//		assertTrue(summary.currSubsequence.get(3) == this.vertex32);
//		assertTrue(summary.isSubsequenceActive());
//		assertTrue(summary.getSubsequenceLatency() == 730.0);
//		assertTrue(summary.getEdges().size() == 3);
//		assertTrue(summary.getEdges().get(0) == this.edge0111);
//		assertTrue(summary.getEdges().get(1) == this.edge1121);
//		assertTrue(summary.getEdges().get(2) == this.edge2132);
//	}
//
//	@Test
//	public void testConstructorWithNoActiveSequence() {
//		this.deactivateEdge(this.edge1020);
//		this.deactivateEdge(this.edge0011);
//		this.deactivateEdge(this.edge2130);
//		this.deactivateEdge(this.edge2131);
//		this.deactivateEdge(this.edge2132);
//		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(
//				this.sequence);
//
//		assertEquals(2, summary.forwardEdgeCounts[0]);
//		assertEquals(2, summary.forwardEdgeCounts[1]);
//		assertEquals(1, summary.forwardEdgeCounts[2]);
//		assertEquals(3, summary.forwardEdgeCounts[3]);
//		assertEquals(-1, summary.forwardEdgeIndices[0]);
//		assertEquals(-1, summary.forwardEdgeIndices[1]);
//		assertEquals(-1, summary.forwardEdgeIndices[2]);
//		assertEquals(-1, summary.forwardEdgeIndices[3]);
//		assertEquals(0, summary.currSubsequence.size());
//		assertFalse(summary.isSubsequenceActive());
//	}
//
//	@Test
//	public void testSwitchToNextActivePath() {
//		this.deactivateEdge(this.edge0011);
//		this.deactivateEdge(this.edge2030);
//		this.deactivateEdge(this.edge2032);
//		this.deactivateEdge(this.edge2130);
//		this.deactivateEdge(this.edge2131);
//		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(
//				this.sequence);
//
//		assertEquals(0, summary.forwardEdgeIndices[0]);
//		assertEquals(0, summary.forwardEdgeIndices[1]);
//		assertEquals(0, summary.forwardEdgeIndices[2]);
//		assertEquals(1, summary.forwardEdgeIndices[3]);
//		assertEquals(4, summary.currSubsequence.size());
//		assertTrue(summary.isSubsequenceActive());
//		assertTrue(summary.currSubsequence.get(0) == this.vertex00);
//		assertTrue(summary.currSubsequence.get(1) == this.vertex10);
//		assertTrue(summary.currSubsequence.get(2) == this.vertex20);
//		assertTrue(summary.currSubsequence.get(3) == this.vertex31);
//		assertTrue(summary.getSubsequenceLatency() == 648.0);
//		assertTrue(summary.getEdges().size() == 3);
//		assertTrue(summary.getEdges().get(0) == this.edge0010);
//		assertTrue(summary.getEdges().get(1) == this.edge1020);
//		assertTrue(summary.getEdges().get(2) == this.edge2031);
//
//		assertTrue(summary.switchToNextActivePathIfPossible());
//		assertEquals(1, summary.forwardEdgeIndices[0]);
//		assertEquals(0, summary.forwardEdgeIndices[1]);
//		assertEquals(0, summary.forwardEdgeIndices[2]);
//		assertEquals(1, summary.forwardEdgeIndices[3]);
//		assertEquals(4, summary.currSubsequence.size());
//		assertTrue(summary.isSubsequenceActive());
//		assertTrue(summary.currSubsequence.get(0) == this.vertex01);
//		assertTrue(summary.currSubsequence.get(1) == this.vertex10);
//		assertTrue(summary.currSubsequence.get(2) == this.vertex20);
//		assertTrue(summary.currSubsequence.get(3) == this.vertex31);
//		assertTrue(summary.getSubsequenceLatency() == 668.0);
//		assertTrue(summary.getEdges().size() == 3);
//		assertTrue(summary.getEdges().get(0) == this.edge0110);
//		assertTrue(summary.getEdges().get(1) == this.edge1020);
//		assertTrue(summary.getEdges().get(2) == this.edge2031);
//
//		assertTrue(summary.switchToNextActivePathIfPossible());
//		assertEquals(1, summary.forwardEdgeIndices[0]);
//		assertEquals(1, summary.forwardEdgeIndices[1]);
//		assertEquals(0, summary.forwardEdgeIndices[2]);
//		assertEquals(2, summary.forwardEdgeIndices[3]);
//		assertEquals(4, summary.currSubsequence.size());
//		assertTrue(summary.isSubsequenceActive());
//		assertTrue(summary.currSubsequence.get(0) == this.vertex01);
//		assertTrue(summary.currSubsequence.get(1) == this.vertex11);
//		assertTrue(summary.currSubsequence.get(2) == this.vertex21);
//		assertTrue(summary.currSubsequence.get(3) == this.vertex32);
//		assertTrue(summary.getSubsequenceLatency() == 730.0);
//		assertTrue(summary.getEdges().size() == 3);
//		assertTrue(summary.getEdges().get(0) == this.edge0111);
//		assertTrue(summary.getEdges().get(1) == this.edge1121);
//		assertTrue(summary.getEdges().get(2) == this.edge2132);
//
//		assertFalse(summary.switchToNextActivePathIfPossible());
//		assertEquals(-1, summary.forwardEdgeIndices[0]);
//		assertEquals(-1, summary.forwardEdgeIndices[1]);
//		assertEquals(-1, summary.forwardEdgeIndices[2]);
//		assertEquals(-1, summary.forwardEdgeIndices[3]);
//		assertEquals(0, summary.currSubsequence.size());
//		assertFalse(summary.isSubsequenceActive());
//	}
//
//	@Test
//	public void addCurrentSubsequenceLatenciesTest() {
//		ProfilingSubsequenceSummary summary = new ProfilingSubsequenceSummary(
//				this.sequence);
//
//		double[] aggregatedLats = new double[8];
//		summary.addCurrentSubsequenceLatencies(aggregatedLats);
//
//		assertTrue(aggregatedLats[0] == 20);
//		assertTrue(aggregatedLats[1] == 90);
//
//		assertTrue(aggregatedLats[2] == 3);
//
//		assertTrue(aggregatedLats[3] == 20);
//		assertTrue(aggregatedLats[4] == 190);
//
//		assertTrue(aggregatedLats[5] == 5);
//
//		assertTrue(aggregatedLats[6] == 20);
//		assertTrue(aggregatedLats[7] == 290);
//	}
//
//	private void deactivateEdge(QosEdge edge) {
//		edge.setQosData(new EdgeQosData(edge) {
//			@Override
//			public boolean isActive() {
//				return false;
//			}
//		});
//	}
}
