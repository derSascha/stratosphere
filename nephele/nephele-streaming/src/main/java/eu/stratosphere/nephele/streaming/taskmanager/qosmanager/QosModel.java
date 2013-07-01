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
package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.message.action.EdgeQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.action.VertexQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.qosreport.EdgeLatency;
import eu.stratosphere.nephele.streaming.message.qosreport.EdgeStatistics;
import eu.stratosphere.nephele.streaming.message.qosreport.QosReport;
import eu.stratosphere.nephele.streaming.message.qosreport.VertexLatency;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGate;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class QosModel {

	public enum State {
		/**
		 * If the Qos model is empty, it means that the internal Qos graph does
		 * not contain any group vertices.
		 */
		EMPTY,

		/**
		 * If the Qos model is shallow, it means that the internal Qos graph
		 * does contain group vertices, but at least one group vertex has no
		 * members. Members are added by vertex/edge announcements piggybacked
		 * inside of Qos reports from the Qos reporters.
		 */
		SHALLOW,

		/**
		 * If the Qos model is ready but incomplete, it means that the internal
		 * Qos graph does contain group vertices, and each group vertex has at
		 * least one member vertex. However, there are still buffered
		 * vertex/edge announcements that cannot be added to the graph. This can
		 * happen two cases: First, the Qos graph may not yet contain all group
		 * vertices necessary to process buffered vertex announcements. Second,
		 * the required source or target vertex of an edge is not yet part of
		 * the Qos graph. In either case, we can have buffered vertex/edge
		 * announcements that cannot be added to the Qos graph.
		 */
		READY_BUT_INCOMPLETE,

		/**
		 * If the Qos model is ready and seemingly complete, it means that the
		 * internal Qos graph does contain group vertices, each group vertex has
		 * at least one member vertex and there no buffered vertex/edge
		 * announcements that cannot be added to the graph.
		 */
		READY_AND_SEEMS_COMPLETE
	}

	private State state;

	/**
	 * A sparse graph that is assembled from two sources: (1) The (shallow)
	 * group-level Qos graphs received as part of the Qos manager roles
	 * delivered by job manager. (2) The vertex/edge reporter announcements
	 * delivered by (possibly many) Qos reporters, once the vertex/edge produces
	 * Qos data (which is may never happen, especially for some edges).
	 */
	private QosGraph qosGraph;

	/**
	 * A dummy Qos report that buffers vertex/edge announcements for later
	 * processing.
	 */
	private QosReport announcementBuffer;

	/**
	 * All gates of the Qos graph mapped by their ID.
	 */
	private HashMap<GateID, QosGate> gatesByGateId;

	/**
	 * All Qos vertices of the Qos graph mapped by their ID.
	 */
	private HashMap<ExecutionVertexID, QosVertex> vertexByID;

	/**
	 * All Qos edges of the Qos graph mapped by their source channel ID.
	 */
	private HashMap<ChannelID, QosEdge> edgeBySourceChannelID;

	public QosModel(JobID jobID) {
		this.state = State.EMPTY;
		this.announcementBuffer = new QosReport(jobID);
		this.gatesByGateId = new HashMap<GateID, QosGate>();
		this.vertexByID = new HashMap<ExecutionVertexID, QosVertex>();
		this.edgeBySourceChannelID = new HashMap<ChannelID, QosEdge>();
	}

	public void mergeShallowQosGraph(QosGraph shallowQosGraph) {
		if (this.qosGraph == null) {
			this.qosGraph = shallowQosGraph;
		} else {
			this.qosGraph.merge(shallowQosGraph);
		}

		
		tryToProcessBufferedAnnouncements();
	}

	public boolean isReady() {
		return this.state == State.READY_BUT_INCOMPLETE
				|| this.state == State.READY_AND_SEEMS_COMPLETE;
	}

	public boolean isEmpty() {
		return this.state == State.EMPTY;
	}

	public boolean isShallow() {
		return this.state == State.SHALLOW;
	}

	public void processQosReport(QosReport report) {
		switch (this.state) {
		case READY_AND_SEEMS_COMPLETE:
			processQosRecords(report);
			if (report.hasAnnouncements()) {
				processOrBufferAnnouncements(report);
				if (this.announcementBuffer.hasAnnouncements()) {
					this.state = State.READY_BUT_INCOMPLETE;
				}
			}
			break;
		case READY_BUT_INCOMPLETE:
			processOrBufferAnnouncements(report);
			processQosRecords(report);
			if (!this.announcementBuffer.hasAnnouncements()) {
				this.state = State.READY_AND_SEEMS_COMPLETE;
			}
			break;
		case SHALLOW:
			processOrBufferAnnouncements(report);
			tryToProcessBufferedAnnouncements();
			break;
		case EMPTY:
			bufferAnnouncements(report);
			break;
		}
	}

	private void processQosRecords(QosReport report) {
		processVertexLatencies(report.getVertexLatencies());
		processEdgeStatistics(report.getEdgeStatistics());
		processEdgeLatencies(report.getEdgeLatencies());
	}

	private void processVertexLatencies(
			Collection<VertexLatency> vertexLatencies) {

		for (VertexLatency vertexLatency : vertexLatencies) {
			QosReporterID.Vertex reporterID = vertexLatency.getReporterID();
			QosGate inputGate = this.gatesByGateId.get(reporterID
					.getInputGateID());

			if (inputGate != null) {
				QosVertex vertex = inputGate.getVertex();
				// FIXME add qos data to vertex
			}
		}
	}

	private void processEdgeStatistics(Collection<EdgeStatistics> edgeStatistics) {
		for (EdgeStatistics edgeStatistic : edgeStatistics) {
			QosReporterID.Edge reporterID = edgeStatistic.getReporterID();
			QosEdge edge = this.edgeBySourceChannelID.get(reporterID
					.getSourceChannelID());

			if (edge != null) {
				// FIXME add qos data to edge
			}
		}
	}

	private void processEdgeLatencies(Collection<EdgeLatency> edgeLatencies) {
		for (EdgeLatency edgeLatency : edgeLatencies) {
			QosReporterID.Edge reporterID = edgeLatency.getReporterID();
			QosEdge edge = this.edgeBySourceChannelID.get(reporterID
					.getSourceChannelID());

			if (edge != null) {
				// FIXME add qos data to edge
			}
		}
	}

	private void processOrBufferAnnouncements(QosReport report) {
		bufferAnnouncements(report);
		tryToProcessBufferedAnnouncements();
	}

	private void tryToProcessBufferedAnnouncements() {
		tryToProcessBufferedVertexReporterAnnouncements();
		tryToProcessBufferedEdgeReporterAnnouncements();
		
		if (this.qosGraph.isShallow()) {
			this.state = State.SHALLOW;
		} else {
			this.state = State.READY_BUT_INCOMPLETE;
		}
	}

	private void tryToProcessBufferedEdgeReporterAnnouncements() {
		Iterator<EdgeQosReporterConfig> vertexIter = this.announcementBuffer
				.getEdgeQosReporterAnnouncements().iterator();

		while (vertexIter.hasNext()) {
			EdgeQosReporterConfig toProcess = vertexIter.next();

			QosGate outputGate = this.gatesByGateId.get(toProcess
					.getOutputGateID());
			QosGate inputGate = this.gatesByGateId.get(toProcess
					.getInputGateID());

			if (inputGate != null && outputGate != null) {
				QosEdge edge = toProcess.toQosEdge();
				outputGate.addEdge(edge);
				inputGate.addEdge(edge);
				vertexIter.remove();
				this.edgeBySourceChannelID.put(edge.getSourceChannelID(), edge);
			}
		}
	}

	private void tryToProcessBufferedVertexReporterAnnouncements() {
		Iterator<VertexQosReporterConfig> vertexIter = this.announcementBuffer
				.getVertexQosReporterAnnouncements().iterator();

		while (vertexIter.hasNext()) {
			VertexQosReporterConfig toProcess = vertexIter.next();
			JobVertexID groupVertexID = toProcess.getGroupVertexID();
			QosGroupVertex groupVertex = this.qosGraph
					.getGroupVertexByID(groupVertexID);

			if (groupVertex != null) {
				int memberIndex = toProcess.getMemberIndex();
				QosVertex memberVertex = groupVertex.getMember(memberIndex);
				if (memberVertex == null) {
					memberVertex = toProcess.toQosVertex();
					groupVertex.setGroupMember(memberVertex);
					this.vertexByID.put(memberVertex.getID(), memberVertex);
				}

				if (toProcess.getInputGateIndex() != -1) {
					QosGate gate = toProcess.toInputGate();
					memberVertex.setInputGate(gate);
					this.gatesByGateId.put(gate.getGateID(), gate);
				}

				if (toProcess.getOutputGateIndex() != -1) {
					QosGate gate = toProcess.toOutputGate();
					memberVertex.setOutputGate(gate);
					this.gatesByGateId.put(gate.getGateID(), gate);
				}

				vertexIter.remove();
			}
		}
	}

	private void bufferAnnouncements(QosReport report) {
		// bufferEdgeLatencies(report.getEdgeLatencies());
		// bufferEdgeStatistics(report.getEdgeStatistics());
		// bufferVertexLatencies(report.getVertexLatencies());
		bufferEdgeQosReporterAnnouncements(report
				.getEdgeQosReporterAnnouncements());
		bufferVertexQosReporterAnnouncements(report
				.getVertexQosReporterAnnouncements());
	}

	private void bufferVertexQosReporterAnnouncements(
			Collection<VertexQosReporterConfig> vertexQosReporterAnnouncements) {

		for (VertexQosReporterConfig reporterConfig : vertexQosReporterAnnouncements) {
			this.announcementBuffer.announceVertexQosReporter(reporterConfig);
		}
	}

	private void bufferEdgeQosReporterAnnouncements(
			List<EdgeQosReporterConfig> edgeQosReporterAnnouncements) {

		for (EdgeQosReporterConfig reporterConfig : edgeQosReporterAnnouncements) {
			this.announcementBuffer.announceEdgeQosReporter(reporterConfig);
		}
	}
}
