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


/**
 * @author Bjoern Lohrmann
 *
 */
public class QosGraphTestUtil {
	
	private QosGraphTestUtil() {
	}

	public static void assertContainsIdentical(QosGroupVertex vertex, QosGraph graph) {
		QosGroupVertex contained = graph.getGroupVertexByID(vertex
				.getJobVertexID());
		assertTrue(vertex == contained);
	}
	
	public static void assertQosGraphEqualToFixture1To5(QosGraph graph, QosGraphFixture fix) {
		assertEquals(5, graph.getNumberOfVertices());
	
		assertContainsEqualButNotIdentical(fix.vertex1, graph);
		assertContainsEqualButNotIdentical(fix.vertex2, graph);
		assertContainsEqualButNotIdentical(fix.vertex3, graph);
		assertContainsEqualButNotIdentical(fix.vertex4, graph);
		assertContainsEqualButNotIdentical(fix.vertex5, graph);
	
		assertEquals(1, graph.getStartVertices().size());
		assertEquals(fix.vertex1, graph.getStartVertices().iterator().next());
		assertEquals(1, graph.getEndVertices().size());
		assertEquals(fix.vertex5, graph.getEndVertices().iterator().next());
	}
	
	public static void assertQosGraphIdenticalToFixture1To5(QosGraph graph, QosGraphFixture fix) {
		assertEquals(5, graph.getNumberOfVertices());
		assertTrue(fix.vertex1 == graph.getGroupVertexByID(fix.vertex1
				.getJobVertexID()));
		assertTrue(fix.vertex2 == graph.getGroupVertexByID(fix.vertex2
				.getJobVertexID()));
		assertTrue(fix.vertex3 == graph.getGroupVertexByID(fix.vertex3
				.getJobVertexID()));
		assertTrue(fix.vertex4 == graph.getGroupVertexByID(fix.vertex4
				.getJobVertexID()));
		assertTrue(fix.vertex5 == graph.getGroupVertexByID(fix.vertex5
				.getJobVertexID()));
	
		assertEquals(1, graph.getStartVertices().size());
		assertTrue(fix.vertex1 == graph.getStartVertices().iterator().next());
		assertEquals(1, graph.getEndVertices().size());
		assertTrue(fix.vertex5 == graph.getEndVertices().iterator().next());
	}
	
	public static void assertContainsEqualButNotIdentical(QosGroupVertex vertex, QosGraph graph) {
		assertContainsEqualButNotIdentical(vertex, graph, true);
	}
	
	public static void assertContainsEqualButNotIdentical(QosGroupVertex vertex,
			QosGraph graph, boolean checkMembers) {
	
		QosGroupVertex contained = graph.getGroupVertexByID(vertex
				.getJobVertexID());
		assertEquals(vertex, contained);
		assertTrue(vertex != contained);
	
		// for(int i=0; i< vertex.getNumberOfOutputGates(); i++) {
		// QosGroupEdge forwardEdge = vertex.getForwardEdge(i);
		// assertEquals(contained.getForwardEdge(i).getTargetVertex(),
		// forwardEdge.getTargetVertex());
		// assertEquals(i, forwardEdge.getOutputGateIndex());
		// }
		//
		// for(int i=0; i< vertex.getNumberOfInputGates(); i++) {
		// QosGroupEdge backwardEdge = vertex.getBackwardEdge(i);
		// assertEquals(contained.getBackwardEdge(i).getSourceVertex(),
		// backwardEdge.getSourceVertex());
		// assertEquals(i, backwardEdge.getInputGateIndex());
		// }
	
		if (checkMembers) {
			assertEquals(contained.getNumberOfMembers(),
					vertex.getNumberOfMembers());
			for (int i = 0; i < contained.getNumberOfMembers(); i++) {
				assertEquals(vertex.getMember(i), contained.getMember(i));
				assertTrue(vertex.getMember(i) != contained.getMember(i));
			}
		}
	}
	
	
	

}
