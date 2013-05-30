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
	
	public JobGraph jobGraph;

	private JobInputVertex jobVertex1;

	private JobTaskVertex jobVertex2;

	private JobTaskVertex jobVertex3;

	private JobTaskVertex jobVertex4;

	private JobGenericOutputVertex jobVertex5;
	
	public ExecutionGraph execGraph;
	
	public QosGraphFixture() throws Exception {
		connectVertices1To5();
		connectVertices0To6();
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
		this.execGraph = new ExecutionGraph(jobGraph, mockInstanceManager);
	}
	
	

	public void makeJobGraph() throws Exception {
		// makes a job graph that contains vertices 1 - 5
		jobGraph = new JobGraph();		
				
		jobVertex1 = new JobInputVertex("vertex1", jobGraph);
		jobVertex1.setInputClass(DummyInputTask.class);
		jobVertex2 = new JobTaskVertex("vertex2", jobGraph);
		jobVertex2.setTaskClass(DummyTask23.class);
		jobVertex3 = new JobTaskVertex("vertex3", jobGraph);
		jobVertex3.setTaskClass(DummyTask23.class);
		jobVertex4 = new JobTaskVertex("vertex4", jobGraph);
		jobVertex4.setTaskClass(DummyTask4.class);
		jobVertex5 = new JobGenericOutputVertex("vertex5", jobGraph);
		jobVertex5.setOutputClass(DummyOutputTask.class);

		jobVertex1.connectTo(jobVertex2, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.BIPARTITE);
		jobVertex1.connectTo(jobVertex3, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.BIPARTITE);
		jobVertex2.connectTo(jobVertex4, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
		jobVertex3.connectTo(jobVertex4, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
		jobVertex4.connectTo(jobVertex5, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.BIPARTITE);
		
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
							InetAddress.getLoopbackAddress(), 1, 1));
			vertex.addGroupMember(member);
		}
	}

	private void generateMemberWiring(QosGroupEdge groupEdge) {
		int sourceMembers = groupEdge.getSourceVertex().getNumberOfMembers();
		int targetMembers = groupEdge.getTargetVertex().getNumberOfMembers();

		for (int i = 0; i < sourceMembers; i++) {
			QosVertex sourceMember = groupEdge.getSourceVertex().getMember(i);
			QosGate outputGate = new QosGate(sourceMember,
					groupEdge.getOutputGateIndex());
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
						inputGate = new QosGate(targetMember,
								groupEdge.getOutputGateIndex());
						targetMember.setInputGate(inputGate);
					}

					QosEdge edge = new QosEdge(new ChannelID(), new ChannelID());
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
