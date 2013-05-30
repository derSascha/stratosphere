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
package eu.stratosphere.nephele.streaming.jobmanager;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class InstanceQosRoles {

	private InstanceConnectionInfo connectionInfo;

	private HashMap<LatencyConstraintID, QosManagerRole> managerRoles;

	private HashSet<QosReporterRole> reporterRoles;

	/**
	 * Initializes QosManagerDescriptor.
	 * 
	 * @param connectionInfo
	 */
	public InstanceQosRoles(InstanceConnectionInfo connectionInfo) {
		this.connectionInfo = connectionInfo;
		this.managerRoles = new HashMap<LatencyConstraintID, QosManagerRole>();
		this.reporterRoles = new HashSet<QosReporterRole>();
	}

	/**
	 * Returns the connectionInfo.
	 * 
	 * @return the connectionInfo
	 */
	public InstanceConnectionInfo getConnectionInfo() {
		return this.connectionInfo;
	}

	public void addManagerRole(QosManagerRole managerRole) {
		if (this.managerRoles.containsKey(managerRole.getConstraintID())) {
			throw new RuntimeException(
					"QoS Manager role for this constraint has already been defined");
		}

		this.managerRoles.put(managerRole.getConstraintID(), managerRole);
	}

	public void addReporterRole(QosReporterRole reporterRole) {
		// it is safe to "just add" here, because the reporter roles hash set
		// checks for equality and equality is defined over all of the role's
		// member variables
		this.reporterRoles.add(reporterRole);
	}

	public Collection<QosManagerRole> getManagerRoles() {
		return this.managerRoles.values();
	}
}
