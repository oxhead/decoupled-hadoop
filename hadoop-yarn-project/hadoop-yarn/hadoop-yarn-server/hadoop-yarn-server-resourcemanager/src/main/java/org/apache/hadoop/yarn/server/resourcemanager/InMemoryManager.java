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
package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.PrefetchInfo;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.PrefetchUtils;

public class InMemoryManager extends AbstractService {

	private static final Log LOG = LogFactory.getLog(InMemoryManager.class);

	private RMContext rmContext;
	private ApplicationMasterService masterService;

	public InMemoryManager(RMContext rmContext, ApplicationMasterService masterService) {
		super(InMemoryManager.class.getName());
		this.rmContext = rmContext;
		this.masterService = masterService;
	}

	@Override
	public synchronized void init(Configuration conf) {
		super.init(conf);
	}

	@Override
	public synchronized void start() {
		super.start();
	}

	@Override
	public synchronized void stop() {
		super.stop();
	}

	private Map<ApplicationId, Set<PrefetchInfo>> appPrefetchRecords = new HashMap<ApplicationId, Set<PrefetchInfo>>();
	private Map<Integer, PrefetchInfo> prefetchRecords = new HashMap<Integer, PrefetchInfo>();

	/**
	 * AM->RM update split requirements
	 * 
	 * @param appId
	 * @param splits
	 */
	public void updateSplitHint(ApplicationId appId, String splits) {
		LOG.error("@@ IMM: update split hint: " + appId);
		LOG.error("@@ IMM: update split hint:" + splits);
		List<PrefetchInfo> prefetchList = PrefetchUtils.decode(splits);
		if (!appPrefetchRecords.containsKey(appId)) {
			appPrefetchRecords.put(appId, new HashSet<PrefetchInfo>());
		} else {
			LOG.fatal("@@ IMM: duplicate split hints");
		}
		for (PrefetchInfo pi : prefetchList) {
			// TODO: could overwrite existing??
			appPrefetchRecords.get(appId).add(pi);
			prefetchRecords.put(pi.hashCode(), pi);
		}
	}

	public PrefetchInfo lookupPrefetch(Integer prefetchId) {
		PrefetchInfo pi = prefetchRecords.get(prefetchId);
		if (pi == null) {
			LOG.fatal("@@ IMM: missing prefetch info");
		}
		return pi;
	}

	/**
	 * NM->RM update prefetching progress
	 * 
	 * @param nodeId
	 * @param splits
	 */
	public void updatePrefetchList(NodeId nodeId, String splits) {
		LOG.error("@@ IMM: update split list -> " + splits);
		List<PrefetchInfo> prefetchList = PrefetchUtils.decode(splits);
		for (PrefetchInfo pi : prefetchList) {
			PrefetchInfo existingPrefetch = lookupPrefetch(pi.hashCode());
			existingPrefetch.node = nodeId.getHost();
			existingPrefetch.progress = pi.progress;
		}
	}

	/**
	 * RM->NM inform of prefetching request
	 * 
	 * @return
	 */
	public String askPrefetchList() {
		LOG.error("@@ IMM: ask prefetch list");
		List<PrefetchInfo> prefetchList = new LinkedList<PrefetchInfo>(prefetchRecords.values());
		return PrefetchUtils.encode(prefetchList);
	}

	/**
	 * RM->AM inform of prefetched splits
	 * 
	 * @return
	 */
	public String getPrefetchSplits(ApplicationId appId) {
		LOG.error("@@ IMM: get prefetch list");
		List<PrefetchInfo> completList = new LinkedList<PrefetchInfo>();
		for (PrefetchInfo pi : prefetchRecords.values()) {
			try {
				LOG.error("@@ IMM: processing split=" + pi);
				int progress = Integer.parseInt(pi.progress);
				int length = Integer.parseInt(pi.fileLength);
				if (progress >= length) {
					completList.add(pi);
				}
			} catch (Exception ex) {
				LOG.error("@@ IMM: fail to process split=" + pi);
			}
		}
		return PrefetchUtils.encode(completList);
	}

}
