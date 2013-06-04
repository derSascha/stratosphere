/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;

import org.mockito.Matchers;

import eu.stratosphere.nephele.executiongraph.DistributionPatternProvider;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.nephele.types.Record;


/**
 * @author Bjoern Lohrmann
 *
 */
public class QosGraphFixture {
	
	public QosGroupVertex vertex1;

	public QosGroupVertex vertex2;

	public QosGroupVertex vertex3;

	public QosGroupVertex vertex4;

	public QosGroupVertex vertex5;

	public QosGroupEdge edge12;

	public QosGroupEdge edge13;

	public QosGroupEdge edge24;

	public QosGroupEdge edge34;

	public QosGroupEdge edge45;

	public QosGroupVertex vertex0;

	public QosGroupVertex vertex1Clone;

	public QosGroupVertex vertex3Clone;

	public QosGroupVertex vertex5Clone;

	public QosGroupVertex vertex6;

	public QosGroupEdge edge05C;

	public QosGroupEdge edge01C;

	public QosGroupEdge edge03C;

	public QosGroupEdge edge5C6;
	
	public QosGroupVertex vertex10;
	
	public QosGroupVertex vertex11;
	
	public QosGroupVertex vertex12;
	
	public QosGroupVertex vertex13;
	
	public QosGroupEdge edge1011; 
	
	public QosGroupEdge edge1112;
	
	public QosGroupEdge edge1213;
	
	public JobGraph jobGraph;

	public JobInputVertex jobVertex1;

	public JobTaskVertex jobVertex2;

	public JobTaskVertex jobVertex3;

	public JobTaskVertex jobVertex4;

	public JobGenericOutputVertex jobVertex5;
	
	public ExecutionGraph execGraph;
	
	/**
	 * Covers e13,v3,e34,v4,e45
	 */
	public JobGraphLatencyConstraint constraint1;

	/**
	 * Covers e12,v2,e24,v4
	 */
	public JobGraphLatencyConstraint constraint2;
	
	/**
	 * Covers v2,e24,v4,e45
	 */
	public JobGraphLatencyConstraint constraint3;
	
	/**
	 * Covers v2,e24,v4
	 */
	public JobGraphLatencyConstraint constraint4;
	
	public QosGraphFixture() throws Exception {
		connectVertices1To5();
		connectVertices0To6();
		connectvertices10To13();
	}

	private void connectvertices10To13() {
		this.vertex10 = new QosGroupVertex(new JobVertexID(), "vertex10");
		this.generateMembers(this.vertex10, 3);

		this.vertex11 = new QosGroupVertex(new JobVertexID(), "vertex11");
		this.generateMembers(this.vertex11, 3);

		this.vertex12 = new QosGroupVertex(new JobVertexID(), "vertex12");
		this.generateMembers(this.vertex12, 3);
		
		this.vertex13 = new QosGroupVertex(new JobVertexID(), "vertex13");
		this.generateMembers(this.vertex13, 3);

		
		this.edge1011 = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex10, this.vertex11, 0, 0);
		this.generateMemberWiring(this.edge1011);
		
		this.edge1112 = new QosGroupEdge(DistributionPattern.POINTWISE,
				this.vertex11, this.vertex12, 0, 0);
		this.generateMemberWiring(this.edge1112);
		
		this.edge1213 = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex12, this.vertex13, 0, 0);
		this.generateMemberWiring(this.edge1213);
	}

	public void makeExecutionGraph() throws Exception {
		mockStatic(ExecutionSignature.class);
		
		ExecutionSignature execSig = mock(ExecutionSignature.class);

		when(ExecutionSignature.createSignature(
				Matchers.eq(AbstractGenericInputTask.class),
						Matchers.any(JobID.class))).thenReturn(execSig);
		when(ExecutionSignature.createSignature(
				Matchers.eq(AbstractTask.class),
				Matchers.any(JobID.class))).thenReturn(execSig);
		when(ExecutionSignature.createSignature(
				Matchers.eq(AbstractOutputTask.class),
				Matchers.any(JobID.class))).thenReturn(execSig);
		
		InstanceManager mockInstanceManager = mock(InstanceManager.class);
		InstanceType instType = new InstanceType();
		when(mockInstanceManager.getDefaultInstanceType()).thenReturn(instType);
		this.execGraph = new ExecutionGraph(this.jobGraph, mockInstanceManager);
	}
	
	public void makeConstraints() {
		/**
		 * Covers e13,v3,e34,v4,e45
		 */
		JobGraphSequence sequence1 = new JobGraphSequence();
		sequence1.addEdge(this.jobVertex1.getID(), 1, this.jobVertex3.getID(), 0);
		sequence1.addVertex(this.jobVertex3.getID(), 0, 0);
		sequence1.addEdge(this.jobVertex3.getID(), 0, this.jobVertex4.getID(), 0);
		sequence1.addVertex(this.jobVertex4.getID(), 0, 0);
		sequence1.addEdge(this.jobVertex4.getID(), 0, this.jobVertex5.getID(), 0);
		this.constraint1 = new JobGraphLatencyConstraint(sequence1, 2000);
		
		/**
		 * Covers e12,v2,e24,v4
		 */
		JobGraphSequence sequence2 = new JobGraphSequence();
		sequence2.addEdge(this.jobVertex1.getID(), 0, this.jobVertex2.getID(), 0);
		sequence2.addVertex(this.jobVertex2.getID(), 0, 0);
		sequence2.addEdge(this.jobVertex2.getID(), 0, this.jobVertex4.getID(), 1);
		sequence2.addVertex(this.jobVertex4.getID(), 1, 0);
		this.constraint2 = new JobGraphLatencyConstraint(sequence2, 2000);

		
		/**
		 * Covers v2,e24,v4,e45
		 */
		JobGraphSequence sequence3 = new JobGraphSequence();
		sequence3.addVertex(this.jobVertex2.getID(), 0, 0);
		sequence3.addEdge(this.jobVertex2.getID(), 0, this.jobVertex4.getID(), 1);
		sequence3.addVertex(this.jobVertex4.getID(), 1, 0);
		sequence3.addEdge(this.jobVertex4.getID(), 0, this.jobVertex5.getID(), 0);
		this.constraint3 = new JobGraphLatencyConstraint(sequence3, 2000);

		
		/**
		 * Covers v2,e24,v4
		 */
		JobGraphSequence sequence4 = new JobGraphSequence();
		sequence4.addVertex(this.jobVertex2.getID(), 0, 0);
		sequence4.addEdge(this.jobVertex2.getID(), 0, this.jobVertex4.getID(), 1);
		sequence4.addVertex(this.jobVertex4.getID(), 1, 0);
		this.constraint4 = new JobGraphLatencyConstraint(sequence4, 2000);
	}

	public void makeJobGraph() throws Exception {
		// makes a job graph that contains vertices 1 - 5
		this.jobGraph = new JobGraph();		
				
		this.jobVertex1 = new JobInputVertex("vertex1", this.jobGraph);
		this.jobVertex1.setInputClass(DummyInputTask.class);
		this.jobVertex1.setNumberOfSubtasks(2);
		this.jobVertex2 = new JobTaskVertex("vertex2", this.jobGraph);
		this.jobVertex2.setTaskClass(DummyTask23.class);
		this.jobVertex2.setNumberOfSubtasks(2);
		this.jobVertex3 = new JobTaskVertex("vertex3", this.jobGraph);
		this.jobVertex3.setTaskClass(DummyTask23.class);
		this.jobVertex3.setNumberOfSubtasks(3);
		this.jobVertex4 = new JobTaskVertex("vertex4", this.jobGraph);
		this.jobVertex4.setTaskClass(DummyTask4.class);
		this.jobVertex4.setNumberOfSubtasks(1);
		this.jobVertex5 = new JobGenericOutputVertex("vertex5", this.jobGraph);
		this.jobVertex5.setOutputClass(DummyOutputTask.class);
		this.jobVertex5.setNumberOfSubtasks(5);

		this.jobVertex1.connectTo(this.jobVertex2, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.BIPARTITE);
		this.jobVertex1.connectTo(this.jobVertex3, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.BIPARTITE);
		this.jobVertex2.connectTo(this.jobVertex4, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
		this.jobVertex3.connectTo(this.jobVertex4, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
		this.jobVertex4.connectTo(this.jobVertex5, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.BIPARTITE);
		
//		File jarFile = File.createTempFile("bogus-test", ".jar");
//		JarFileCreator jfc = new JarFileCreator(jarFile);
//		jfc.addClass(AbstractGenericInputTask.class);
//		jfc.addClass(AbstractTask.class);
//		jfc.addClass(AbstractOutputTask.class);
//		jfc.createJarFile();
//		jobGraph.addJar(new Path(jarFile.getAbsolutePath()));
	}

	private void connectVertices0To6() {
		// vertices 0, 1Clone, 3Clone, 5Clone and 6 form a graph to test merging
		this.vertex0 = new QosGroupVertex(new JobVertexID(), "vertex0");
		this.generateMembers(this.vertex0, 15);

		this.vertex1Clone = this.vertex1.cloneWithoutEdges();

		this.vertex3Clone = this.vertex3.cloneWithoutEdges();

		this.vertex5Clone = this.vertex5.cloneWithoutEdges();

		this.vertex6 = new QosGroupVertex(new JobVertexID(), "vertex6");
		this.generateMembers(this.vertex6, 3);

		this.edge05C = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex0, this.vertex5Clone, 0, 1);
		this.generateMemberWiring(this.edge05C);

		this.edge01C = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex0, this.vertex1Clone, 1, 0);
		this.generateMemberWiring(this.edge01C);

		this.edge03C = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex0, this.vertex3Clone, 2, 1);
		this.generateMemberWiring(this.edge03C);

		this.edge5C6 = new QosGroupEdge(DistributionPattern.POINTWISE,
				this.vertex5Clone, this.vertex6, 2, 1);
		this.generateMemberWiring(this.edge5C6);
	}

	private void connectVertices1To5() {
		// vertex 1-5 form a graph
		this.vertex1 = new QosGroupVertex(new JobVertexID(), "vertex1");
		this.generateMembers(this.vertex1, 2);

		this.vertex2 = new QosGroupVertex(new JobVertexID(), "vertex2");
		this.generateMembers(this.vertex2, 2);

		this.vertex3 = new QosGroupVertex(new JobVertexID(), "vertex3");
		this.generateMembers(this.vertex3, 3);

		this.vertex4 = new QosGroupVertex(new JobVertexID(), "vertex4");
		this.generateMembers(this.vertex4, 1);

		this.vertex5 = new QosGroupVertex(new JobVertexID(), "vertex5");
		this.generateMembers(this.vertex5, 4);

		this.edge12 = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex1, this.vertex2, 0, 0);
		this.generateMemberWiring(this.edge12);

		this.edge13 = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex1, this.vertex3, 1, 0);
		this.generateMemberWiring(this.edge13);

		this.edge24 = new QosGroupEdge(DistributionPattern.POINTWISE,
				this.vertex2, this.vertex4, 0, 0);
		this.generateMemberWiring(this.edge24);

		this.edge34 = new QosGroupEdge(DistributionPattern.POINTWISE,
				this.vertex3, this.vertex4, 0, 1);
		this.generateMemberWiring(this.edge34);

		this.edge45 = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex4, this.vertex5, 0, 0);
		this.generateMemberWiring(this.edge45);
	}

	private void generateMembers(QosGroupVertex vertex, int memberCount) {
		for (int i = 0; i < memberCount; i++) {
			QosVertex member = new QosVertex(new ExecutionVertexID(),
					vertex.getName() + "_" + i, new InstanceConnectionInfo(
							InetAddress.getLoopbackAddress(), 1, 1), i);
			vertex.setGroupMember(member);
		}
	}

	private void generateMemberWiring(QosGroupEdge groupEdge) {
		int sourceMembers = groupEdge.getSourceVertex().getNumberOfMembers();
		int targetMembers = groupEdge.getTargetVertex().getNumberOfMembers();

		for (int i = 0; i < sourceMembers; i++) {
			QosVertex sourceMember = groupEdge.getSourceVertex().getMember(i);
			QosGate outputGate = new QosGate(groupEdge.getOutputGateIndex());
			sourceMember.setOutputGate(outputGate);

			for (int j = 0; j < targetMembers; j++) {
				if (DistributionPatternProvider.createWire(
						groupEdge.getDistributionPattern(), i, j,
						sourceMembers, targetMembers)) {

					QosVertex targetMember = groupEdge.getTargetVertex()
							.getMember(j);

					QosGate inputGate = targetMember.getInputGate(groupEdge
							.getInputGateIndex());
					if (inputGate == null) {
						inputGate = new QosGate(groupEdge.getOutputGateIndex());
						targetMember.setInputGate(inputGate);
					}

					QosEdge edge = new QosEdge(new ChannelID(),
							new ChannelID(), outputGate.getNumberOfEdges(),
							inputGate.getNumberOfEdges());
					edge.setInputGate(inputGate);
					edge.setOutputGate(outputGate);
				}
			}
		}
	}
	
	public static class DummyRecord implements Record {
		@Override
		public void write(DataOutput out) throws IOException {
		}

		@Override
		public void read(DataInput in) throws IOException {
		}		
	}
	
	
	public void assertContainsIdentical(QosGroupVertex vertex, QosGraph graph) {
		QosGroupVertex contained = graph.getGroupVertexByID(vertex
				.getJobVertexID());
		assertTrue(vertex == contained);
	}

	public void assertQosGraphEqualToFixture1To5(QosGraph graph) {
		assertEquals(5, graph.getNumberOfVertices());

		this.assertContainsEqualButNotIdentical(this.vertex1, graph);
		this.assertContainsEqualButNotIdentical(this.vertex2, graph);
		this.assertContainsEqualButNotIdentical(this.vertex3, graph);
		this.assertContainsEqualButNotIdentical(this.vertex4, graph);
		this.assertContainsEqualButNotIdentical(this.vertex5, graph);

		assertEquals(1, graph.getStartVertices().size());
		assertEquals(this.vertex1, graph.getStartVertices().iterator()
				.next());
		assertEquals(1, graph.getEndVertices().size());
		assertEquals(this.vertex5, graph.getEndVertices().iterator().next());
	}
	
	public void assertQosGraphIdenticalToFixture1To5(QosGraph graph) {
		assertEquals(5, graph.getNumberOfVertices());
		assertTrue(this.vertex1 == graph
				.getGroupVertexByID(this.vertex1.getJobVertexID()));
		assertTrue(this.vertex2 == graph
				.getGroupVertexByID(this.vertex2.getJobVertexID()));
		assertTrue(this.vertex3 == graph
				.getGroupVertexByID(this.vertex3.getJobVertexID()));
		assertTrue(this.vertex4 == graph
				.getGroupVertexByID(this.vertex4.getJobVertexID()));
		assertTrue(this.vertex5 == graph
				.getGroupVertexByID(this.vertex5.getJobVertexID()));

		assertEquals(1, graph.getStartVertices().size());
		assertTrue(this.vertex1 == graph.getStartVertices().iterator()
				.next());
		assertEquals(1, graph.getEndVertices().size());
		assertTrue(this.vertex5 == graph.getEndVertices().iterator().next());
	}

	public void assertContainsEqualButNotIdentical(QosGroupVertex vertex,
			QosGraph graph) {
		this.assertContainsEqualButNotIdentical(vertex, graph, true);
	}

	public void assertContainsEqualButNotIdentical(QosGroupVertex vertex,
			QosGraph graph, boolean checkMembers) {

		QosGroupVertex contained = graph.getGroupVertexByID(vertex
				.getJobVertexID());
		assertEquals(vertex, contained);
		assertTrue(vertex != contained);
		
//		for(int i=0; i< vertex.getNumberOfOutputGates(); i++) {
//			QosGroupEdge forwardEdge = vertex.getForwardEdge(i);
//			assertEquals(contained.getForwardEdge(i).getTargetVertex(), forwardEdge.getTargetVertex());
//			assertEquals(i, forwardEdge.getOutputGateIndex());
//		}
//		
//		for(int i=0; i< vertex.getNumberOfInputGates(); i++) {
//			QosGroupEdge backwardEdge = vertex.getBackwardEdge(i);
//			assertEquals(contained.getBackwardEdge(i).getSourceVertex(), backwardEdge.getSourceVertex());
//			assertEquals(i, backwardEdge.getInputGateIndex());
//		}		
		
		if (checkMembers) {
			assertEquals(contained.getNumberOfMembers(),
					vertex.getNumberOfMembers());
			for (int i = 0; i < contained.getNumberOfMembers(); i++) {
				assertEquals(vertex.getMember(i), contained.getMember(i));
				assertTrue(vertex.getMember(i) != contained.getMember(i));
			}
		}
	}
	
	public static class DummyInputTask extends AbstractGenericInputTask {
				
		@SuppressWarnings("unused")
		@Override
		public void registerInputOutput() {
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
		}

		@Override
		public void invoke() throws Exception {
		}		
		
		@Override
		public GenericInputSplit[] computeInputSplits(int foo) {
			return new GenericInputSplit[0];
		}
	}
	
	public static class DummyOutputTask extends AbstractOutputTask {
		
		@SuppressWarnings("unused")
		@Override
		public void registerInputOutput() {
			new RecordReader<DummyRecord>(this, DummyRecord.class);
		}

		@Override
		public void invoke() throws Exception {
		}		
	}
	
	public static class DummyTask23 extends AbstractTask {
		
		@SuppressWarnings("unused")
		@Override
		public void registerInputOutput() {
			new RecordReader<DummyRecord>(this, DummyRecord.class);
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
		}

		@Override
		public void invoke() throws Exception {
		}		
	}
	
	public static class DummyTask4 extends AbstractTask {
		
		@SuppressWarnings("unused")
		@Override
		public void registerInputOutput() {
			new RecordReader<DummyRecord>(this, DummyRecord.class);
			new RecordReader<DummyRecord>(this, DummyRecord.class);
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
		}

		@Override
		public void invoke() throws Exception {
		}		
	}

	

}
