package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.EdgeCharacteristics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingSequence;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingVertex;

public class ProfilingSubsequenceSummary {

	protected ProfilingSequence sequence;

	protected ArrayList<ProfilingVertex> currSubsequence;

	/**
	 * For an active subsequence, this list contains all the sequence's edges,
	 * sorted by descending latency.
	 */
	protected ArrayList<ProfilingEdge> edges;

	/**
	 * The i-th element is the forward edge index that connects the
	 * currSubsequence.get(i-1) with currSubsequence.get(i). In other words:
	 * currSubsequence
	 * .get(i-1).getForwardEdges().get(forwardEdgeIndices).getTargetVertex() ==
	 * currSubsequence.get(i) For i=0, the forward edge index is the index of
	 * currSubsequence.get(0) in the first group vertex of the sequence.
	 */
	protected int[] forwardEdgeIndices;

	/**
	 * The i-th element is the number of forward edges of
	 * currSubsequence.get(i-1). In other words:
	 * currSubsequence.get(i-1).getForwardEdges().size() == forwardEdgeCounts[i]
	 * For i=0, the forward edge count is the number of vertices in the first
	 * group vertex of the sequence.
	 */
	protected int[] forwardEdgeCounts;

	protected int sequenceDepth;

	protected boolean currSubsequenceActive;

	protected long subsequenceLatency;

	protected double[] subsequenceElementLatencies;

	protected int noOfActiveSubsequencesFound;

	public ProfilingSubsequenceSummary(ProfilingSequence sequence) {
		this.sequence = sequence;
		this.noOfActiveSubsequencesFound = 0;
		this.sequenceDepth = this.sequence.getSequenceVertices().size();
		this.currSubsequence = new ArrayList<ProfilingVertex>();
		this.initForwardEdgeCounts();
		this.initForwardEdgeIndices();
		this.initEdges();
		this.initSubsequenceElementLatencies();

		// find first active path
		this.findNextActivePath(false);
	}

	private void initSubsequenceElementLatencies() {
		int size = 2 * (this.sequence.getSequenceVertices().size() - 1)
				+ this.sequence.getSequenceVertices().size();
		if (!this.sequence.isIncludeStartVertex()) {
			size--;
		}
		if (!this.sequence.isIncludeEndVertex()) {
			size--;
		}
		this.subsequenceElementLatencies = new double[size];
	}

	private void initEdges() {
		this.edges = new ArrayList<ProfilingEdge>();
		// init with nulls, so that sortEdgesByLatency() can use ArrayList.set()
		// without clearing the list
		for (int i = 0; i < this.sequenceDepth - 1; i++) {
			this.edges.add(null);
		}
	}

	protected void initForwardEdgeCounts() {
		this.forwardEdgeCounts = new int[this.sequenceDepth];
		for (int i = 0; i < this.forwardEdgeCounts.length; i++) {
			if (i == 0) {
				this.forwardEdgeCounts[i] = this.sequence.getSequenceVertices()
						.get(0).getGroupMembers().size();
			} else {
				this.forwardEdgeCounts[i] = this.sequence.getSequenceVertices()
						.get(i - 1).getGroupMembers().get(0).getForwardEdges()
						.size();
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
		this.currSubsequenceActive = this.recursiveFindNextActivePath(0,
				resumePathEnumeration);
		if (this.currSubsequenceActive) {
			this.noOfActiveSubsequencesFound++;
			this.computeLatency();
			this.collectEdges();
		}
	}

	private void collectEdges() {
		for (int i = 0; i < this.sequenceDepth - 1; i++) {
			this.edges.set(i, this.currSubsequence.get(i).getForwardEdges()
					.get(this.forwardEdgeIndices[i + 1]));
		}
	}

	/**
	 * @param depth
	 * @return true when active path found, false otherwise.
	 */
	protected boolean recursiveFindNextActivePath(final int depth,
			final boolean resumePathEnumeration) {
		boolean activePathFound = false;
		if (resumePathEnumeration) {
			if (depth < this.sequenceDepth - 1) {
				// recurse deeper to resume
				activePathFound = this.recursiveFindNextActivePath(depth + 1,
						true);
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
				vertexToAdd = this.sequence.getSequenceVertices().get(0)
						.getGroupMembers().get(this.forwardEdgeIndices[depth]);
			} else {
				edgeToAdd = this.currSubsequence.get(depth - 1)
						.getForwardEdges().get(this.forwardEdgeIndices[depth]);
				vertexToAdd = edgeToAdd.getTargetVertex();
			}

			boolean edgeNullOrActive = edgeToAdd == null
					|| this.isActive(edgeToAdd);
			boolean vertexActiveOrExcluded = depth == 0
					&& !this.sequence.isIncludeStartVertex() || depth == 0
					&& !this.sequence.isIncludeStartVertex()
					|| depth == this.sequenceDepth - 1
					&& !this.sequence.isIncludeEndVertex()
					|| this.isActive(vertexToAdd);

			if (edgeNullOrActive && vertexActiveOrExcluded) {
				this.currSubsequence.add(vertexToAdd);

				if (depth < this.sequenceDepth - 1) {
					activePathFound = this.recursiveFindNextActivePath(
							depth + 1, false);
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
			this.findNextActivePath(true);
		}
		return this.currSubsequenceActive;
	}

	private void computeLatency() {
		this.subsequenceLatency = 0;

		int vertexIndex = 0;
		int insertPosition = 0;

		if (this.sequence.isIncludeStartVertex()) {
			this.addLatency(insertPosition,
					this.currSubsequence.get(vertexIndex).getVertexLatency()
							.getLatencyInMillis());
			insertPosition++;
		}
		this.addChannelAndOutputBufferLatency(
				insertPosition,
				this.currSubsequence.get(vertexIndex).getForwardEdges()
						.get(this.forwardEdgeIndices[vertexIndex + 1])
						.getEdgeCharacteristics());
		insertPosition += 2;
		vertexIndex++;

		while (insertPosition < this.subsequenceElementLatencies.length) {
			ProfilingVertex vertex = this.currSubsequence.get(vertexIndex);

			this.addLatency(insertPosition, vertex.getVertexLatency()
					.getLatencyInMillis());
			insertPosition++;

			if (vertex.getForwardEdges() != null) {
				EdgeCharacteristics fwEdgeCharacteristics = vertex
						.getForwardEdges()
						.get(this.forwardEdgeIndices[vertexIndex + 1])
						.getEdgeCharacteristics();

				this.addChannelAndOutputBufferLatency(insertPosition,
						fwEdgeCharacteristics);
				insertPosition += 2;
			}

			vertexIndex++;
		}
	}

	private void addChannelAndOutputBufferLatency(int insertPosition,
			EdgeCharacteristics fwEdgeCharacteristics) {
		double outputBufferLatency = fwEdgeCharacteristics
				.getOutputBufferLifetimeInMillis() / 2;
		this.subsequenceElementLatencies[insertPosition] = outputBufferLatency;
		this.subsequenceLatency += outputBufferLatency;

		// channel latency includes output buffer latency, hence we subtract the
		// output buffer latency
		// in order not to count it twice
		double remainingChannelLatency = Math.max(0,
				fwEdgeCharacteristics.getChannelLatencyInMillis()
						- outputBufferLatency);
		this.subsequenceElementLatencies[insertPosition + 1] = remainingChannelLatency;
		this.subsequenceLatency += remainingChannelLatency;
	}

	private void addLatency(int insertPosition, double vertexLatency) {
		this.subsequenceElementLatencies[insertPosition] = vertexLatency;
		this.subsequenceLatency += vertexLatency;
	}

	public List<ProfilingVertex> getVertices() {
		return this.currSubsequence;
	}

	public int getNoOfActiveSubsequencesFound() {
		return this.noOfActiveSubsequencesFound;
	}

	public double getSubsequenceLatency() {
		return this.subsequenceLatency;
	}

	public void addCurrentSubsequenceLatencies(double[] aggregatedLatencies) {
		for (int i = 0; i < aggregatedLatencies.length; i++) {
			aggregatedLatencies[i] += this.subsequenceElementLatencies[i];
		}
	}

	public List<ProfilingEdge> getEdges() {
		return this.edges;
	}
}
