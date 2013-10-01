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
package eu.stratosphere.nephele.streaming.taskmanager.runtime.io;

import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.jobmanager.CandidateChainFinder;
import eu.stratosphere.nephele.streaming.jobmanager.CandidateChainListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphFactory;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphFixture;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphFixture2;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.LinkedList;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link eu.stratosphere.nephele.streaming.jobmanager.CandidateChainFinder}
 *
 */
@PrepareForTest({   ExecutionSignature.class, AbstractInstance.class,
                    AllocatedResource.class})
@RunWith(PowerMockRunner.class)
public class CandidateChainFinderTest {
    private QosGraphFixture fix;
    private QosGraphFixture2 fix2;

    @Mock
    private CandidateChainListener listener;

    @Before
    public void setUp() throws Exception {
        this.fix = new QosGraphFixture();
        fix2 = new QosGraphFixture2();
    }

    @Test
    public void testSomething() throws Exception {
        CandidateChainFinder candidateChainFinder =
                new CandidateChainFinder(this.listener, this.fix.execGraph);
        JobGraphLatencyConstraint constraint = this.fix.constraint1;
        QosGraph constrainedQosGraph =
                QosGraphFactory.createConstrainedQosGraph(this.fix.execGraph, constraint);
        candidateChainFinder.findChainsAlongConstraint(constraint.getID(), constrainedQosGraph);
        verify(this.listener, atLeastOnce()).handleCandidateChain(
                any(InstanceConnectionInfo.class), any(LinkedList.class)
        );
    }

    /**
     * TODO callback is not called is that correct for the given
     * constraint?
     *
     * @throws Exception
     */
    @Test
    public void testPositiveCon4() throws Exception {

        QosGraph constrainedQosGraph = QosGraphFactory.createConstrainedQosGraph(
                this.fix.execGraph, this.fix.constraint4);
        CandidateChainFinder f = new CandidateChainFinder(this.listener, this.fix.execGraph);
        f.findChainsAlongConstraint(this.fix.constraint4.getID(), constrainedQosGraph);
        verify(this.listener, atLeastOnce()).handleCandidateChain(
                any(InstanceConnectionInfo.class), any(LinkedList.class));
    }

    /**
     * TODO: ditto (@link #testPositiveCon4}
     *
     * @throws Exception
     */
    @Test
    public void testPositiveCon3() throws Exception {
        QosGraph constrainedQosGraph = QosGraphFactory.createConstrainedQosGraph(
                this.fix.execGraph, this.fix.constraint3
        );
        CandidateChainFinder f = new CandidateChainFinder(this.listener, this.fix.execGraph);
        f.findChainsAlongConstraint(this.fix.constraint3.getID(), constrainedQosGraph);
        verify(this.listener, atLeastOnce()).handleCandidateChain(
                any(InstanceConnectionInfo.class), any(LinkedList.class));
    }

    /**
     * -> V -> should not cause a chain.
     *
     * @throws Exception
     */
    @Test
    public void testEdgeVertexEdge() throws Exception {
        QosGraph constrainedQosGraph = QosGraphFactory.createConstrainedQosGraph(
                this.fix2.executionGraph, this.fix2.constraintEdgeVertexEdge
        );
        CandidateChainFinder f = new CandidateChainFinder(this.listener, this.fix2.executionGraph);
        f.findChainsAlongConstraint(this.fix2.constraintEdgeVertexEdge.getID(), constrainedQosGraph);
        verify(listener, never()).handleCandidateChain(
                any(InstanceConnectionInfo.class),
                any(LinkedList.class)
        );
    }

    /**
     * Tests a subgraph t1 -> t2 ->
     *
     * TODO ist das korrekt? t1 hat mehrere Eingangskanten
     *
     * @throws Exception
     */
    @Test
    public void testVertexEdgeVertexEdge() throws Exception {
        QosGraph constrainedQosGraph = QosGraphFactory.createConstrainedQosGraph(
                this.fix2.executionGraph, this.fix2.constraintVertexEdgeVertexEdge
        );
        CandidateChainFinder f = new CandidateChainFinder(this.listener, this.fix2.executionGraph);
        f.findChainsAlongConstraint(this.fix2.constraintVertexEdgeVertexEdge.getID(), constrainedQosGraph);
        verify(listener, times(1)).handleCandidateChain(
                any(InstanceConnectionInfo.class),
                any(LinkedList.class)
        );
    }

    /**
     * Tests a sequence of the full graph described in {@link QosGraphFixture2}.
     *
     * @throws Exception
     */
    @Test
    public void testFullSequence() throws Exception {
        QosGraph constrainedQosGraph = QosGraphFactory.createConstrainedQosGraph(
                fix2.executionGraph, fix2.constraintFull
        );
        CandidateChainFinder f = new CandidateChainFinder(listener, fix2.executionGraph);
        f.findChainsAlongConstraint(fix2.constraintFull.getID(), constrainedQosGraph);
        verify(listener, atLeastOnce()).handleCandidateChain(
                any(InstanceConnectionInfo.class), any(LinkedList.class)
        );
    }
}
