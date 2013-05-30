package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.util.Iterator;

import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingSequence;

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
		this.countTotalNoOfSubsequences();
		this.createAggregatedElementLatencies();
		this.avgSubsequenceLatency = 0;
		this.minSubsequenceLatency = Double.MAX_VALUE;
		this.maxSubsequenceLatency = Double.MIN_VALUE;
		this.enumeratingSummary = new ProfilingSubsequenceSummary(sequence);
		if (this.enumeratingSummary.isSubsequenceActive()) {
			this.updateAggregatedFields();
		} else {
			this.finalizeAggregatedFields();
		}
	}

	private void createAggregatedElementLatencies() {
		// 2 values per edge and one for each vertex
		int size = 2 * (this.sequence.getSequenceVertices().size() - 1)
				+ this.sequence.getSequenceVertices().size();
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
		for (QosGroupVertex groupVertex : this.sequence
				.getSequenceVertices()) {
			if (index == 0) {
				this.totalNoOfSubsequences = groupVertex.getMembers()
						.size();
			} else if (groupVertex.getBackwardEdge().getDistributionPattern() == DistributionPattern.BIPARTITE) {
				this.totalNoOfSubsequences *= groupVertex.getMembers()
						.size();
			}

			index++;
		}
	}

	public Iterable<ProfilingSubsequenceSummary> enumerateActiveSubsequences() {
		return new Iterable<ProfilingSubsequenceSummary>() {
			@Override
			public Iterator<ProfilingSubsequenceSummary> iterator() {
				return new Iterator<ProfilingSubsequenceSummary>() {
					private boolean done = !ProfilingSequenceSummary.this.enumeratingSummary
							.isSubsequenceActive();

					private boolean hasNext = ProfilingSequenceSummary.this.enumeratingSummary
							.isSubsequenceActive();

					private boolean currentHasBeenReturned = false;

					@Override
					public boolean hasNext() {
						if (!this.done && this.currentHasBeenReturned) {
							this.hasNext = ProfilingSequenceSummary.this.enumeratingSummary
									.switchToNextActivePathIfPossible();
							if (this.hasNext) {
								ProfilingSequenceSummary.this
										.updateAggregatedFields();
								this.currentHasBeenReturned = false;
							} else {
								this.done = true;
								ProfilingSequenceSummary.this
										.finalizeAggregatedFields();
							}
						}
						return this.hasNext;
					}

					@Override
					public ProfilingSubsequenceSummary next() {
						if (this.currentHasBeenReturned) {
							return null;
						}
						this.currentHasBeenReturned = true;
						return ProfilingSequenceSummary.this.enumeratingSummary;
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
		this.minSubsequenceLatency = Math.min(latency,
				this.minSubsequenceLatency);
		this.maxSubsequenceLatency = Math.max(latency,
				this.maxSubsequenceLatency);
		this.enumeratingSummary
				.addCurrentSubsequenceLatencies(this.avgSequenceElementLatencies);
	}

	private void finalizeAggregatedFields() {
		if (this.enumeratingSummary.getNoOfActiveSubsequencesFound() > 0) {
			this.avgSubsequenceLatency /= this.enumeratingSummary
					.getNoOfActiveSubsequencesFound();
			for (int i = 0; i < this.avgSequenceElementLatencies.length; i++) {
				this.avgSequenceElementLatencies[i] /= this.enumeratingSummary
						.getNoOfActiveSubsequencesFound();
			}
		} else {
			this.avgSubsequenceLatency = 0;
			this.minSubsequenceLatency = 0;
			this.maxSubsequenceLatency = 0;
		}
	}

	public ProfilingSequence getSequence() {
		return this.sequence;
	}

	public int getNoOfInactiveSubsequences() {
		return this.totalNoOfSubsequences
				- this.enumeratingSummary.getNoOfActiveSubsequencesFound();
	}

	public int getTotalNoOfSubsequences() {
		return this.totalNoOfSubsequences;
	}

	public double getAvgSubsequenceLatency() {
		return this.avgSubsequenceLatency;
	}

	public double getMinSubsequenceLatency() {
		return this.minSubsequenceLatency;
	}

	public double getMaxSubsequenceLatency() {
		return this.maxSubsequenceLatency;
	}

	public double getMedianPathLatency() {
		throw new UnsupportedOperationException();
	}

	public double[] getAvgSequenceElementLatencies() {
		return this.avgSequenceElementLatencies;
	}

	public Object getNoOfActiveSubsequences() {
		return this.enumeratingSummary.getNoOfActiveSubsequencesFound();
	}

	public ProfilingSequence getProfilingSequence() {
		return this.sequence;
	}
}
