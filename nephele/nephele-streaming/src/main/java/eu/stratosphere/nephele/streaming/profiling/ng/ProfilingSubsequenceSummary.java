package eu.stratosphere.nephele.streaming.profiling.ng;

import java.util.ArrayList;
import java.util.Collection;

import eu.stratosphere.nephele.streaming.profiling.model.ProfilingEdge;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingSequence;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingVertex;

public class ProfilingSubsequenceSummary {

	protected ProfilingSequence sequence;

	protected ArrayList<ProfilingVertex> currSubsequence;

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
		initForwardEdgeCounts();
		findFirstActivePath();
	}

	protected void initForwardEdgeCounts() {
		this.sequenceDepth = this.sequence.getSequenceVertices().size();

		this.forwardEdgeCounts = new int[this.sequenceDepth];
		for (int i = 0; i < forwardEdgeCounts.length; i++) {
			if (i == 0) {
				this.forwardEdgeCounts[i] = this.sequence.getSequenceVertices().get(0).getGroupMembers().size();
			} else {
				this.forwardEdgeCounts[i] = sequence.getSequenceVertices().get(i - 1).getGroupMembers().get(0)
					.getForwardEdges().size();
			}
		}

		// for (int i = 0; i < this.forwardEdgeIndices.length; i++) {
		// this.forwardEdgeIndices[i] = 0;
		//
		// ProfilingVertex toAdd;
		// if (i == 0) {
		// toAdd = this.sequence.getSequenceVertices().get(0).getGroupMembers().get(0);
		// } else {
		// toAdd = this.currSubsequence.get(i - 1).getForwardEdges().get(0).getTargetVertex();
		// }
		// currSubsequence.add(toAdd);
		// }
	}

	protected void findFirstActivePath() {
		this.currSubsequence = new ArrayList<ProfilingVertex>();
		this.forwardEdgeIndices = new int[this.sequenceDepth];

		for (int i = 0; i < this.forwardEdgeIndices.length; i++) {
			this.forwardEdgeIndices[i] = -1;
		}
		findNextActivePath(false);
	}

	protected void findNextActivePath(boolean resumePathEnumeration) {
		this.currSubsequenceActive = recursiveFindNextActivePath(0, resumePathEnumeration);
		if (this.currSubsequenceActive) {
			this.noOfActiveSubsequencesFound++;
			computeLatency();
		}
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

	public Collection<ProfilingVertex> getVertices() {
		return this.currSubsequence;
	}

	public int getNoOfActiveSubsequencesFound() {
		return noOfActiveSubsequencesFound;
	}

	public double getSubsequenceLatency() {
		return subsequenceLatency;
	}	
}
