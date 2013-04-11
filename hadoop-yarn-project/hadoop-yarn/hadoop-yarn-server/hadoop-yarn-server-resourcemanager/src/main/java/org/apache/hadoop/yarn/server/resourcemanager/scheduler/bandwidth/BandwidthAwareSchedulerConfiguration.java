/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.bandwidth;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;

public class BandwidthAwareSchedulerConfiguration extends Configuration {

	private static final Log LOG = LogFactory.getLog(BandwidthAwareSchedulerConfiguration.class);

	private static final String CONFIGURATION_FILE = "bandwidth-aware-scheduler.xml";

	@Private
	public static final String PREFIX = "yarn.scheduler.bandwidth-aware.";

	@Private
	public static final String DOT = ".";

	@Private
	public static final String BALANCING = PREFIX + "balancing";

	@Private
	public static final String FLOWNETWORK_TASKS_MAX = PREFIX + "flownetwork.tasks.max";

	@Private
	public static final String FLOWNETWORK_PERIOD = PREFIX + "flownetwork.period";

	@Private
	public static final String CAPACITY_APPMASTER = PREFIX + "capacity.appmaster";

	@Private
	public static final String CAPACITY_REDUCE = PREFIX + "capacity.reduce";

	@Private
	public static final String CAPACITY_TERASORT = PREFIX + "capacity.terasort";

	@Private
	public static final String CAPACITY_WORDCOUNT = PREFIX + "capacity.wordcount";

	@Private
	public static final String CAPACITY_GREP = PREFIX + "capacity.grep";

	@Private
	public static final int CAPACITY_DEFAULT = 50;

	public BandwidthAwareSchedulerConfiguration() {
		this(new Configuration());
	}

	public BandwidthAwareSchedulerConfiguration(Configuration configuration) {
		super(configuration);
		addResource(CONFIGURATION_FILE);
	}

	public enum Type {
		APPMASTER, TERASORT, WORDCOUNT, GREP, REDUCE;
	}

	public boolean getBalancing() {
		return getBoolean(BALANCING, false);
	}

	public int getDefaultCapacity() {
		return CAPACITY_DEFAULT;
	}

	public int getCapacity(Type type) {
		int capacity = getDefaultCapacity();

		if (type.equals(Type.TERASORT)) {
			capacity = getInt(CAPACITY_TERASORT, CAPACITY_DEFAULT);
		} else if (type.equals(Type.WORDCOUNT)) {
			capacity = getInt(CAPACITY_WORDCOUNT, CAPACITY_DEFAULT);
		} else if (type.equals(Type.GREP)) {
			capacity = getInt(CAPACITY_GREP, CAPACITY_DEFAULT);
		}

		return capacity;
	}

	public int getFlowNetworkPeriod() {
		int period = getInt(FLOWNETWORK_PERIOD, 5);
		return period;
	}

	public int getMaxNumOfSchedulingTasks() {
		int maxNumOfSchedulingTasks = getInt(FLOWNETWORK_TASKS_MAX, 100);
		return maxNumOfSchedulingTasks;
	}
}