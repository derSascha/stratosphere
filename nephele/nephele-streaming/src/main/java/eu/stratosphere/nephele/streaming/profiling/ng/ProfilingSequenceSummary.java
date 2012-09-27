package eu.stratosphere.nephele.streaming.profiling.ng;

import java.util.Iterator;

import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingGroupVertex;
import eu.stratosphere.nephele.streaming.profiling.model.ProfilingSequence;

public class ProfilingSequenceSummary {

	private ProfilingSequence sequence;

	private int totalNoOfSubsequences;

	private ProfilingSubsequenceSummary enumeratingSummary;

	private double avgSubsequenceLatency;

	private double minSubsequenceLatency;

	private double maxSubsequenceLatency;

	private double[] avgSequenceElementLatencies;

	public ProfilingSequenceSummary(ProfilingSequence sequence) {
		this.sequence = sequence;
		countTotalNoOfSubsequences();
		createAggregatedElementLatencies();
		this.enumeratingSummary = new ProfilingSubsequenceSummary(sequence);
		this.avgSubsequenceLatency = 0;
		this.minSubsequenceLatency = Double.MAX_VALUE;
		this.minSubsequenceLatency = Double.MIN_VALUE;
	}

	private void createAggregatedElementLatencies() {
		// vertices and edges
		int size = 2 * this.sequence.getSequenceVertices().size() - 1;
		if (!this.sequence.isIncludeStartVertex()) {
			size--;
		}
		if (!this.sequence.isIncludeEndVertex()) {
			size--;
		}
		this.avgSequenceElementLatencies = new double[size];
	}

	private void countTotalNoOfSubsequences() {
		int index = 0;
		for (ProfilingGroupVertex groupVertex : sequence.getSequenceVertices()) {
			if (index == 0) {
				this.totalNoOfSubsequences = groupVertex.getGroupMembers().size();
			} else if (groupVertex.getBackwardEdge().getDistributionPattern() == DistributionPattern.BIPARTITE) {
				this.totalNoOfSubsequences *= groupVertex.getGroupMembers().size();
			}

			index++;
		}
	}

	public Iterable<ProfilingSubsequenceSummary> enumerateSubsequenceSummaries() {
		return new Iterable<ProfilingSubsequenceSummary>() {
			@Override
			public Iterator<ProfilingSubsequenceSummary> iterator() {
				return new Iterator<ProfilingSubsequenceSummary>() {
					private boolean done = false;

					@Override
					public boolean hasNext() {
						boolean hasNext = false;
						if (!done) {
							hasNext = enumeratingSummary.switchToNextActivePathIfPossible();
							if (hasNext) {
								updateAggregatedFields();
							} else {
								done = true;
								finalizeAggregatedFields();
							}

						}
						return hasNext;
					}

					@Override
					public ProfilingSubsequenceSummary next() {
						return enumeratingSummary;
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
	}

	private void updateAggregatedFields() {
		double latency = this.enumeratingSummary.getSubsequenceLatency();
		this.avgSubsequenceLatency += latency;
		this.minSubsequenceLatency = Math.min(latency, minSubsequenceLatency);
		this.maxSubsequenceLatency = Math.max(latency, maxSubsequenceLatency);

		// TODO: hier weitermachen.
	}

	private void finalizeAggregatedFields() {
		this.avgSubsequenceLatency /= enumeratingSummary.getNoOfActiveSubsequencesFound();

	}

	public ProfilingSequence getSequence() {
		return sequence;
	}

	public int getNoOfInactiveSubsequences() {
		return totalNoOfSubsequences - enumeratingSummary.getNoOfActiveSubsequencesFound();
	}

	public int getTotalNoOfSubsequences() {
		return totalNoOfSubsequences;
	}

	public double getAvgSubsequenceLatency() {
		return avgSubsequenceLatency;
	}

	public double getMinSubsequenceLatency() {
		return minSubsequenceLatency;
	}

	public double getMaxSubsequenceLatency() {
		return maxSubsequenceLatency;
	}

	public double getMedianPathLatency() {
		throw new UnsupportedOperationException();
	}

	public double[] getAvgSequenceElementLatencies() {
		return this.avgSequenceElementLatencies;
	}
}
