package eu.stratosphere.nephele.streaming.profiling.model;

import eu.stratosphere.nephele.io.DistributionPattern;

public class ProfilingGroupEdge {

	private DistributionPattern distributionPattern;

	private ProfilingGroupVertex sourceVertex;

	private ProfilingGroupVertex targetVertex;

	public ProfilingGroupEdge(DistributionPattern distributionPattern,
			ProfilingGroupVertex sourceVertex, ProfilingGroupVertex targetVertex) {
		this.distributionPattern = distributionPattern;
		this.sourceVertex = sourceVertex;
		this.targetVertex = targetVertex;
	}

	public DistributionPattern getDistributionPattern() {
		return this.distributionPattern;
	}

	public ProfilingGroupVertex getSourceVertex() {
		return this.sourceVertex;
	}

	public ProfilingGroupVertex getTargetVertex() {
		return this.targetVertex;
	}
}
