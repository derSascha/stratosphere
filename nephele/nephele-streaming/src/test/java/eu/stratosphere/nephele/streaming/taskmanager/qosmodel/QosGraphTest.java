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

import org.junit.Before;
import org.junit.Test;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class QosGraphTest {

	private QosGraphFixture fix;

	@Before
	public void setup() throws Exception {
		this.fix = new QosGraphFixture();
	}

	@Test
	public void testConstructorWithStartVertex() {
		QosGraph graph = new QosGraph(this.fix.vertex1);
		this.fix.assertQosGraphIdenticalToFixture1To5(graph);
	}

	@Test
	public void testMergeForwardWithEmptyGraph() {
		QosGraph graph = new QosGraph();
		graph.mergeForwardReachableGroupVertices(this.fix.vertex1);
		this.fix.assertQosGraphEqualToFixture1To5(graph);
	}

	@Test
	public void testMergeForwardWithNonemptyGraph() {
		QosGraph graph = new QosGraph(this.fix.vertex1);
		graph.mergeForwardReachableGroupVertices(this.fix.vertex0);
		this.assertMergedFixtureGraphs(graph);
	}

	/**
	 * @param graph
	 */
	private void assertMergedFixtureGraphs(QosGraph graph) {
		assertEquals(7, graph.getNumberOfVertices());
		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex0, graph);
		this.fix.assertContainsIdentical(this.fix.vertex1, graph);
		this.fix.assertContainsIdentical(this.fix.vertex2, graph);
		this.fix.assertContainsIdentical(this.fix.vertex3, graph);
		this.fix.assertContainsIdentical(this.fix.vertex4, graph);
		this.fix.assertContainsIdentical(this.fix.vertex5, graph);
		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex6, graph);

		assertEquals(1,
				graph.getGroupVertexByID(this.fix.vertex1.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(2,
				graph.getGroupVertexByID(this.fix.vertex3.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(2,
				graph.getGroupVertexByID(this.fix.vertex5.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(1,
				graph.getGroupVertexByID(this.fix.vertex5.getJobVertexID())
						.getNumberOfOutputGates());

		assertEquals(1, graph.getStartVertices().size());
		assertEquals(this.fix.vertex0, graph.getStartVertices().iterator()
				.next());
		assertEquals(1, graph.getEndVertices().size());
		assertEquals(this.fix.vertex6, graph.getEndVertices().iterator().next());
	}

	@Test
	public void testMergeBackwardEmptyGraph() {
		QosGraph graph = new QosGraph();
		graph.mergeBackwardReachableGroupVertices(this.fix.vertex5);
		this.fix.assertQosGraphEqualToFixture1To5(graph);
	}

	@Test
	public void testMergeBackwardWithNonemptyGraph() {
		QosGraph graph = new QosGraph(this.fix.vertex1);
		graph.mergeBackwardReachableGroupVertices(this.fix.vertex6);

		assertEquals(7, graph.getNumberOfVertices());
		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex0, graph);
		this.fix.assertContainsIdentical(this.fix.vertex1, graph);
		this.fix.assertContainsIdentical(this.fix.vertex2, graph);
		this.fix.assertContainsIdentical(this.fix.vertex3, graph);
		this.fix.assertContainsIdentical(this.fix.vertex4, graph);
		this.fix.assertContainsIdentical(this.fix.vertex5, graph);
		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex6, graph);

		assertEquals(1,
				graph.getGroupVertexByID(this.fix.vertex0.getJobVertexID())
						.getNumberOfOutputGates());
		assertEquals(0,
				graph.getGroupVertexByID(this.fix.vertex1.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(1,
				graph.getGroupVertexByID(this.fix.vertex3.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(2,
				graph.getGroupVertexByID(this.fix.vertex5.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(1,
				graph.getGroupVertexByID(this.fix.vertex5.getJobVertexID())
						.getNumberOfOutputGates());

		assertEquals(2, graph.getStartVertices().size());
		assertTrue(graph.getStartVertices().contains(this.fix.vertex0));
		assertTrue(graph.getStartVertices().contains(this.fix.vertex1));
		assertEquals(1, graph.getEndVertices().size());
		assertEquals(this.fix.vertex6, graph.getEndVertices().iterator().next());
	}

	@Test
	public void testMergeIntoEmptyGraph() {
		QosGraph graph = new QosGraph();
		graph.merge(new QosGraph(this.fix.vertex1));
		this.fix.assertQosGraphEqualToFixture1To5(graph);
	}

	@Test
	public void testMergeEmptyIntoNonEmptyGraph() {
		QosGraph graph = new QosGraph(this.fix.vertex1);
		graph.merge(new QosGraph());
		this.fix.assertQosGraphIdenticalToFixture1To5(graph);
	}

	@Test
	public void testMergeNonEmptyIntoNonEmptyGraph() {
		QosGraph graph = new QosGraph(this.fix.vertex1);
		graph.merge(new QosGraph(this.fix.vertex0));
		this.assertMergedFixtureGraphs(graph);
	}

	@Test
	public void testCloneWithoutMembers() {
		QosGraph orig = new QosGraph(this.fix.vertex10);

		QosGraph clone = orig.cloneWithoutMembers();
		assertEquals(4, clone.getNumberOfVertices());

		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex10, clone, false);
		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex11, clone, false);
		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex12, clone, false);
		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex13, clone, false);

		assertEquals(1, clone.getStartVertices().size());
		assertEquals(this.fix.vertex10, clone.getStartVertices().iterator()
				.next());
		assertEquals(1, clone.getEndVertices().size());
		assertEquals(this.fix.vertex13, clone.getEndVertices().iterator().next());
	}

}
