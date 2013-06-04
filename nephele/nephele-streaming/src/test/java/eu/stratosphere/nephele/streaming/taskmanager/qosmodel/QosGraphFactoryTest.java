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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.executiongraph.ExecutionSignature;

/**
 * @author Bjoern Lohrmann
 *
 */
@PrepareForTest(ExecutionSignature.class)
@RunWith(PowerMockRunner.class)
public class QosGraphFactoryTest {
	
	private QosGraphFixture fix;
	
	@Before
	public void setup() throws Exception {
		this.fix = new QosGraphFixture();
		this.fix.makeJobGraph();
		this.fix.makeExecutionGraph();
		this.fix.makeConstraints();
	}
	
	@Test
	public void testCreateConstrainedQosGraphWithConstraint1() throws Exception {		
		QosGraph graph = QosGraphFactory.createConstrainedQosGraph(this.fix.execGraph, this.fix.constraint1);
		assertEquals(4, graph.getNumberOfVertices());

		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex1, graph);
		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex3, graph);
		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex4, graph);
		this.fix.assertContainsEqualButNotIdentical(this.fix.vertex5, graph);

		assertEquals(1, graph.getStartVertices().size());
		assertEquals(this.fix.vertex1, graph.getStartVertices().iterator()
				.next());
		assertEquals(1, graph.getEndVertices().size());
		assertEquals(this.fix.vertex5, graph.getEndVertices().iterator().next());
	}
}
