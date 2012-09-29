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
		this.avgSubsequenceLatency = 0;
		this.minSubsequenceLatency = Double.MAX_VALUE;
		this.maxSubsequenceLatency = Double.MIN_VALUE;
		this.enumeratingSummary = new ProfilingSubsequenceSummary(sequence);
		if (this.enumeratingSummary.isSubsequenceActive()) {
			updateAggregatedFields();
		}
	}

	private void createAggregatedElementLatencies() {
		// 2 values per edge and one for each vertex
		int size = 2 * (this.sequence.getSequenceVertices().size() - 1) + this.sequence.getSequenceVertices().size();
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

	public Iterable<ProfilingSubsequenceSummary> enumerateActiveSubsequences() {
		return new Iterable<ProfilingSubsequenceSummary>() {
			@Override
			public Iterator<ProfilingSubsequenceSummary> iterator() {
				return new Iterator<ProfilingSubsequenceSummary>() {
					private boolean done = !enumeratingSummary.isSubsequenceActive();

					private boolean hasNext = enumeratingSummary.isSubsequenceActive();

					private boolean currentHasBeenReturned = false;

					@Override
					public boolean hasNext() {
						if (!done && currentHasBeenReturned) {
							hasNext = enumeratingSummary.switchToNextActivePathIfPossible();
							if (hasNext) {
								updateAggregatedFields();
								currentHasBeenReturned = false;
							} else {
								done = true;
								finalizeAggregatedFields();
							}
						}
						return hasNext;
					}

					@Override
					public ProfilingSubsequenceSummary next() {
						if (currentHasBeenReturned) {
							return null;
						} else {
							currentHasBeenReturned = true;
							return enumeratingSummary;
						}
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
		this.minSubsequenceLatency = Math.min(latency, this.minSubsequenceLatency);
		this.maxSubsequenceLatency = Math.max(latency, this.maxSubsequenceLatency);
		this.enumeratingSummary.addCurrentSubsequenceLatencies(this.avgSequenceElementLatencies);
	}

	private void finalizeAggregatedFields() {
		if (enumeratingSummary.getNoOfActiveSubsequencesFound() > 0) {
			this.avgSubsequenceLatency /= enumeratingSummary.getNoOfActiveSubsequencesFound();
			for (int i = 0; i < this.avgSequenceElementLatencies.length; i++) {
				this.avgSequenceElementLatencies[i] /= enumeratingSummary.getNoOfActiveSubsequencesFound();
			}
		} else {
			this.avgSubsequenceLatency = 0;
			this.minSubsequenceLatency = 0;
			this.maxSubsequenceLatency = 0;
		}
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

	public Object getNoOfActiveSubsequences() {
		return enumeratingSummary.getNoOfActiveSubsequencesFound();
	}

	public ProfilingSequence getProfilingSequence() {
		return sequence;
	}
}
