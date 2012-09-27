package eu.stratosphere.nephele.streaming.profiling.ng;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import eu.stratosphere.nephele.streaming.profiling.EdgeCharacteristics;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingEdge;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingSequence;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingVertex;

public class ProfilingSubsequenceSummary {

	protected ProfilingSequence sequence;

	protected ArrayList<ProfilingVertex> currSubsequence;

	/**
	 * For an active subsequence, this list contains all the sequence's edges, sorted
	 * by descending latency.
	 */
	protected ArrayList<ProfilingEdge> edgesSortedByLatency;

	private Comparator<ProfilingEdge> edgeComparator = new Comparator<ProfilingEdge>() {
		@Override
		public int compare(ProfilingEdge first, ProfilingEdge second) {
			double firstLatency = first.getEdgeCharacteristics().getChannelLatencyInMillis();
			double secondLatency = second.getEdgeCharacteristics().getChannelLatencyInMillis();

			if (firstLatency < secondLatency) {
				return 1;
			} else if (firstLatency > secondLatency) {
				return -1;
			} else {
				return 0;
			}
		}
	};

	/**
	 * The i-th element is the forward edge index that connects the currSubsequence.get(i-1)
	 * with currSubsequence.get(i). In other words:
	 * currSubsequence.get(i-1).getForwardEdges().get(forwardEdgeIndices).getTargetVertex() == currSubsequence.get(i)
	 * For i=0, the forward edge index is the index of currSubsequence.get(0) in the first group vertex of the sequence.
	 */
	protected int[] forwardEdgeIndices;

	/**
	 * The i-th element is the number of forward edges of currSubsequence.get(i-1). In other words:
	 * currSubsequence.get(i-1).getForwardEdges().size() == forwardEdgeCounts[i]
	 * For i=0, the forward edge count is the number of vertices in the first group vertex of the sequence.
	 */
	protected int[] forwardEdgeCounts;

	protected int sequenceDepth;

	protected boolean currSubsequenceActive;

	protected long subsequenceLatency;

	protected int noOfActiveSubsequencesFound;

	public ProfilingSubsequenceSummary(ProfilingSequence sequence) {
		this.sequence = sequence;
		this.noOfActiveSubsequencesFound = 0;
		this.sequenceDepth = this.sequence.getSequenceVertices().size();
		this.currSubsequence = new ArrayList<ProfilingVertex>();
		initForwardEdgeCounts();
		initForwardEdgeIndices();
		initEdgesSortedByLatency();

		// find first active path
		findNextActivePath(false);
	}

	private void initEdgesSortedByLatency() {
		this.edgesSortedByLatency = new ArrayList<ProfilingEdge>();
		// init with nulls, so that sortEdgesByLatency() can use ArrayList.set()
		// without clearing the list
		for (int i = 0; i < this.sequenceDepth - 1; i++) {
			this.edgesSortedByLatency.add(null);
		}
	}

	protected void initForwardEdgeCounts() {
		this.forwardEdgeCounts = new int[this.sequenceDepth];
		for (int i = 0; i < forwardEdgeCounts.length; i++) {
			if (i == 0) {
				this.forwardEdgeCounts[i] = this.sequence.getSequenceVertices().get(0).getGroupMembers().size();
			} else {
				this.forwardEdgeCounts[i] = sequence.getSequenceVertices().get(i - 1).getGroupMembers().get(0)
					.getForwardEdges().size();
			}
		}
	}

	private void initForwardEdgeIndices() {
		this.forwardEdgeIndices = new int[this.sequenceDepth];
		for (int i = 0; i < this.forwardEdgeIndices.length; i++) {
			this.forwardEdgeIndices[i] = -1;
		}
	}

	protected void findNextActivePath(boolean resumePathEnumeration) {
		this.currSubsequenceActive = recursiveFindNextActivePath(0, resumePathEnumeration);
		if (this.currSubsequenceActive) {
			this.noOfActiveSubsequencesFound++;
			computeLatency();
			sortEdgesByLatency();
		}
	}

	private void sortEdgesByLatency() {
		for (int i = 0; i < this.sequenceDepth - 1; i++) {
			edgesSortedByLatency.set(i,
				this.currSubsequence.get(i).getForwardEdges().get(this.forwardEdgeIndices[i + 1]));
		}
		Collections.sort(edgesSortedByLatency, edgeComparator);
	}

	/**
	 * @param depth
	 * @return true when active path found, false otherwise.
	 */
	protected boolean recursiveFindNextActivePath(final int depth, final boolean resumePathEnumeration) {
		boolean activePathFound = false;
		if (resumePathEnumeration) {
			if (depth < this.sequenceDepth - 1) {
				// recurse deeper to resume
				activePathFound = recursiveFindNextActivePath(depth + 1, true);
			}
			if (!activePathFound) {
				this.currSubsequence.remove(depth);
			}
		}

		while (!activePathFound) {

			this.forwardEdgeIndices[depth]++;
			if (this.forwardEdgeIndices[depth] >= this.forwardEdgeCounts[depth]) {
				this.forwardEdgeIndices[depth] = -1;
				break; // no active path found
			}

			ProfilingEdge edgeToAdd;
			ProfilingVertex vertexToAdd;

			if (depth == 0) {
				edgeToAdd = null;
				vertexToAdd = this.sequence.getSequenceVertices().get(0).getGroupMembers()
					.get(this.forwardEdgeIndices[depth]);
			} else {
				edgeToAdd = this.currSubsequence.get(depth - 1).getForwardEdges().get(this.forwardEdgeIndices[depth]);
				vertexToAdd = edgeToAdd.getTargetVertex();
			}

			boolean edgeNullOrActive = edgeToAdd == null || isActive(edgeToAdd);
			boolean vertexActiveOrExcluded = (depth == 0 && !sequence.isIncludeStartVertex())
				|| (depth == 0 && !sequence.isIncludeStartVertex())
				|| (depth == this.sequenceDepth - 1 && !sequence.isIncludeEndVertex())
				|| isActive(vertexToAdd);

			if (edgeNullOrActive && vertexActiveOrExcluded) {
				this.currSubsequence.add(vertexToAdd);

				if (depth < this.sequenceDepth - 1) {
					activePathFound = recursiveFindNextActivePath(depth + 1, false);
					if (!activePathFound) {
						this.currSubsequence.remove(depth);
					}
				} else {
					activePathFound = true;
				}
			}
		}

		return activePathFound;
	}

	protected boolean isActive(ProfilingVertex vertex) {
		return vertex.getVertexLatency().isActive();
	}

	protected boolean isActive(ProfilingEdge edge) {
		return edge.getEdgeCharacteristics().isActive();
	}

	public boolean isSubsequenceActive() {
		return this.currSubsequenceActive;
	}

	public boolean switchToNextActivePathIfPossible() {
		if (this.currSubsequenceActive) {
			findNextActivePath(true);
		}
		return this.currSubsequenceActive;
	}

	private void computeLatency() {
		this.subsequenceLatency = 0;

		int loopEnd = this.currSubsequence.size();
		if (!sequence.isIncludeEndVertex()) {
			loopEnd--;
		}
		for (int i = 0; i < loopEnd; i++) {
			ProfilingVertex vertex = this.currSubsequence.get(i);
			if (i > 0 || this.sequence.isIncludeStartVertex()) {
				this.subsequenceLatency += vertex.getVertexLatency().getLatencyInMillis();
			}

			ProfilingEdge edge = vertex.getForwardEdges().get(this.forwardEdgeIndices[i + 1]);
			this.subsequenceLatency += edge.getEdgeCharacteristics().getChannelLatencyInMillis();
		}
	}

	public List<ProfilingVertex> getVertices() {
		return this.currSubsequence;
	}

	public int getNoOfActiveSubsequencesFound() {
		return noOfActiveSubsequencesFound;
	}

	public double getSubsequenceLatency() {
		return subsequenceLatency;
	}

	public void addCurrentSubsequenceLatencies(double[] aggregatedLatencies) {

		int vertexIndex = 0;
		int insertPosition = 0;

		while (insertPosition < aggregatedLatencies.length) {
			ProfilingVertex vertex = currSubsequence.get(vertexIndex);

			if (vertexIndex == 0 && this.sequence.isIncludeStartVertex()) {
				aggregatedLatencies[insertPosition] += vertex.getVertexLatency().getLatencyInMillis();
				insertPosition++;
			}

			if (vertex.getForwardEdges() != null) {
				EdgeCharacteristics fwEdgeCharacteristics = vertex.getForwardEdges()
					.get(this.forwardEdgeIndices[vertexIndex + 1]).getEdgeCharacteristics();

				aggregatedLatencies[insertPosition] += fwEdgeCharacteristics.getOutputBufferLifetimeInMillis() / 2;
				insertPosition++;

				// channel latency includes output buffer latency, hence we subtract the output buffer latency
				// in order not to count it twice
				aggregatedLatencies[insertPosition] += fwEdgeCharacteristics.getChannelLatencyInMillis()
					- aggregatedLatencies[insertPosition - 1];
				insertPosition++;
			}

			vertexIndex++;
		}
	}

	public List<ProfilingEdge> getEdgesSortedByLatency() {
		return this.edgesSortedByLatency;
	}
}
