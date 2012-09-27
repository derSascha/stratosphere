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

package eu.stratosphere.nephele.streaming.actions;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.types.AbstractStreamingData;

/**
 * This class implements an abstract base class for actions the job manager component of the Nephele streaming plugin
 * can initiate to achieve particular latency or throughput goals.
 * 
 * @author warneke
 */
public abstract class AbstractAction extends AbstractStreamingData {

	/**
	 * Constructs a new abstract action object.
	 * 
	 * @param jobID
	 *        the ID of the job the initiated action applies to
	 */
	AbstractAction(final JobID jobID) {
		super(jobID);
	}

	/**
	 * Default constructor required for deserialization.
	 */
	AbstractAction() {
	}
}
