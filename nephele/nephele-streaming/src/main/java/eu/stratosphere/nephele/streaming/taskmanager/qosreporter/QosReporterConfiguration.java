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
package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class QosReporterConfiguration {

	private long aggregationInterval;

	private int taggingInterval;

	/**
	 * Returns the aggregationInterval.
	 * 
	 * @return the aggregationInterval
	 */
	public long getAggregationInterval() {
		return this.aggregationInterval;
	}

	/**
	 * Sets the aggregationInterval to the specified value.
	 * 
	 * @param aggregationInterval
	 *            the aggregationInterval to set
	 */
	public void setAggregationInterval(long aggregationInterval) {
		this.aggregationInterval = aggregationInterval;
	}

	/**
	 * Returns the taggingInterval.
	 * 
	 * @return the taggingInterval
	 */
	public int getTaggingInterval() {
		return this.taggingInterval;
	}

	/**
	 * Sets the taggingInterval to the specified value.
	 * 
	 * @param taggingInterval
	 *            the taggingInterval to set
	 */
	public void setTaggingInterval(int taggingInterval) {
		this.taggingInterval = taggingInterval;
	}

}
