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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.im;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.Lock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.PrefetchInfo;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.util.BuilderUtils;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class InMemoryScheduler implements ResourceScheduler, Configurable {

	private static final Log LOG = LogFactory.getLog(InMemoryScheduler.class);

	private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

	Configuration conf;

	private final static Container[] EMPTY_CONTAINER_ARRAY = new Container[] {};
	private final static List<Container> EMPTY_CONTAINER_LIST = Arrays.asList(EMPTY_CONTAINER_ARRAY);
	private RMContext rmContext;

	private Map<NodeId, FiCaSchedulerNode> nodes = new ConcurrentHashMap<NodeId, FiCaSchedulerNode>();

	private boolean initialized;
	private Resource minimumAllocation;
	private Resource maximumAllocation;

	private Map<ApplicationAttemptId, FiCaSchedulerApp> applications = new TreeMap<ApplicationAttemptId, FiCaSchedulerApp>();

	private ActiveUsersManager activeUsersManager;

	private static final String DEFAULT_QUEUE_NAME = "default";
	private QueueMetrics metrics;

	private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

	private InMemoryManager inMemoryManager;
	private ApplicationSplitManager applicationSplitManager;
	private long prefetchBlockSize;
	private int prefetchWindow;

	// This method must be called for all the map tasks from AM
	public void addPrefetchRequest(ApplicationAttemptId applicationAttemptId, List<PrefetchInfo> prefetchList) {
		LOG.error("@@ Scheduler: add prefetch request: " + applicationAttemptId + ", size=" + prefetchList.size());

		for (PrefetchInfo pi : prefetchList) {
			this.applicationSplitManager.addApplicationSplitRecord(applicationAttemptId.getApplicationId(), pi);
		}
	}

	public void reportCompletedPrefetchTask(ApplicationAttemptId applicationAttemptId, List<PrefetchInfo> prefetchList) {
		LOG.error("@@ Scheduler: report completed prefetch tasks: " + applicationAttemptId + ", size=" + prefetchList.size());
		for (PrefetchInfo pi : prefetchList) {
			SplitContext splitContext = this.applicationSplitManager.lookupSplitContext(pi);
			splitContext.setTaskCompleted();
			this.inMemoryManager.revokePrefetchSplit(splitContext);
		}

	}

	public void reportFailedPrefetchTask(ApplicationAttemptId applicationAttemptId, List<PrefetchInfo> prefetchList) {
		LOG.error("@@ Scheduler: report failed prefetch tasks: " + applicationAttemptId + ", size=" + prefetchList.size());
		for (PrefetchInfo pi : prefetchList) {
			SplitContext splitContext = this.applicationSplitManager.lookupSplitContext(pi);
			ApplicationSplitConext applicationContext = this.applicationSplitManager.lookupApplication(applicationAttemptId.getApplicationId());
			applicationContext.addSplit(splitContext);
		}

	}

	public List<PrefetchInfo> getAssignedPrefetchSplits(ApplicationId appId) {
		List<SplitContext> assignedSplitContextList = this.applicationSplitManager.lookupApplication(appId).pullAssignedTasks();
		List<PrefetchInfo> assignedTaskList = new LinkedList<PrefetchInfo>();
		for (SplitContext splitContext : assignedSplitContextList) {
			assignedTaskList.add(splitContext.pi);
		}
		return assignedTaskList;
	}

	// NM->RM
	public void updatePrefetchProgress(NodeId nodeId, List<PrefetchInfo> prefetchList) {
		LOG.error("@@ Scheduler: update prefetch progress: " + nodeId + ", size=" + prefetchList.size());
		for (PrefetchInfo pi : prefetchList) {
			SplitContext as = this.applicationSplitManager.lookupSplitContext(pi);
			this.inMemoryManager.updatePrefetchProgress(as, pi.progress);
		}
	}

	public void updateNodePrefetchWindow(NodeId nodeId, int prefetchWindow) {
		LOG.error("@@ Scheduler: update prefetch windows: " + nodeId + ", window=" + prefetchWindow);
		this.inMemoryManager.updatePrefetchWindow(nodeId, prefetchWindow);
	}

	public List<PrefetchInfo> pullPendingPrefetchList(NodeId nodeId, int availableNum) {
		NodeSplitContext nodeContext = this.inMemoryManager.lookupNode(nodeId);
		List<PrefetchInfo> prefetchList = new LinkedList<PrefetchInfo>();
		for (SplitContext splitContext : nodeContext.pullPrefetchingTasks()) {
			prefetchList.add(splitContext.pi);
			splitContext.setPrefetchStarted();
			splitContext.prefetchedBytes = 0;
		}
		LOG.error("@@ Scheduler: pull pending prefetch tasks -> node=" + nodeId + ", avail=" + availableNum + ", num=" + prefetchList.size());
		return prefetchList;
	}

	public List<PrefetchInfo> pullCompletedPrefetchList(NodeId nodeId) {
		NodeSplitContext nodeContext = this.inMemoryManager.lookupNode(nodeId);
		LOG.error("@@ Scheduler: revoke prefetching tasks -> node=" + nodeId + ", num=" + nodeContext.revokedTaskQueue.size());

		List<PrefetchInfo> prefetchList = new LinkedList<PrefetchInfo>();
		for (SplitContext as : nodeContext.pullCompletedPrefetchTask()) {
			prefetchList.add(as.pi);
		}
		LOG.error("@@ Scheduler: pull completed prefetch tasks -> node=" + nodeId + ", num=" + prefetchList.size());
		return prefetchList;
	}

	private final Queue DEFAULT_QUEUE = new Queue() {
		@Override
		public String getQueueName() {
			return DEFAULT_QUEUE_NAME;
		}

		@Override
		public QueueMetrics getMetrics() {
			return metrics;
		}

		@Override
		public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
			QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
			queueInfo.setQueueName(DEFAULT_QUEUE.getQueueName());
			queueInfo.setCapacity(1.0f);
			if (clusterResource.getMemory() == 0) {
				queueInfo.setCurrentCapacity(0.0f);
			} else {
				queueInfo.setCurrentCapacity((float) usedResource.getMemory() / clusterResource.getMemory());
			}
			queueInfo.setMaximumCapacity(1.0f);
			queueInfo.setChildQueues(new ArrayList<QueueInfo>());
			queueInfo.setQueueState(QueueState.RUNNING);
			return queueInfo;
		}

		@Override
		public Map<QueueACL, AccessControlList> getQueueAcls() {
			Map<QueueACL, AccessControlList> acls = new HashMap<QueueACL, AccessControlList>();
			for (QueueACL acl : QueueACL.values()) {
				acls.put(acl, new AccessControlList("*"));
			}
			return acls;
		}

		@Override
		public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation unused) {
			QueueUserACLInfo queueUserAclInfo = recordFactory.newRecordInstance(QueueUserACLInfo.class);
			queueUserAclInfo.setQueueName(DEFAULT_QUEUE_NAME);
			queueUserAclInfo.setUserAcls(Arrays.asList(QueueACL.values()));
			return Collections.singletonList(queueUserAclInfo);
		}
	};

	@Override
	public synchronized void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public synchronized Configuration getConf() {
		return conf;
	}

	@Override
	public Resource getMinimumResourceCapability() {
		return minimumAllocation;
	}

	@Override
	public int getNumClusterNodes() {
		return nodes.size();
	}

	@Override
	public Resource getMaximumResourceCapability() {
		return maximumAllocation;
	}

	@Override
	public synchronized void reinitialize(Configuration conf, RMContext rmContext) throws IOException {
		setConf(conf);
		if (!this.initialized) {
			this.rmContext = rmContext;
			this.minimumAllocation = Resources.createResource(conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
			this.maximumAllocation = Resources.createResource(conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
			this.metrics = QueueMetrics.forQueue(DEFAULT_QUEUE_NAME, null, false, conf);
			this.activeUsersManager = new ActiveUsersManager(metrics);
			this.initialized = true;
			this.prefetchBlockSize = conf.getLong("fs.local.block.size", 64 * 2014 * 1024);
			this.prefetchWindow = conf.getInt(YarnConfiguration.IM_PREFETCH_WINDOW, YarnConfiguration.DEFAULT_IM_PREFETCH_WINDOW);
			this.applicationSplitManager = new ApplicationSplitManager();
			this.inMemoryManager = new InMemoryManager();

			new Thread() {

				@Override
				public void run() {
					while (true) {
						try {
							schedule();
							Thread.sleep(1000);
						} catch (Exception ex) {
							LOG.error("@@ Scheduler: scheduling failed", ex);
						}
					}
				}

			}.start();
		}
	}

	private void schedule() {
		LOG.error("@@ Scheduler: start to schedule");
		int beforeMemory = 0;
		int afterMemory = 0;
		int beforeSplitNum = this.applicationSplitManager.getUnscheduledSplit();
		for (FiCaSchedulerNode node : nodes.values()) {
			LOG.error("Node before allocation " + node.getNodeID() + " resource = " + node.getAvailableResource());
			beforeMemory += node.getAvailableResource().getMemory();
			assignContainers(node);
			LOG.error("Node after allocation " + node.getNodeID() + " resource = " + node.getAvailableResource());
			afterMemory += node.getAvailableResource().getMemory();
		}
		LOG.error("@@ Scheduler: allocation statistics -> before=" + beforeMemory + ", after=" + afterMemory);
		LOG.error("@@ Scheduler: task statistics -> before=" + beforeSplitNum + ", after=" + this.applicationSplitManager.getUnscheduledSplit());
		this.applicationSplitManager.printSplitList();
		LOG.error("@@ Scheduler: prefetching statistics -> " + this.inMemoryManager.prefetchQueue.size());
		this.inMemoryManager.printPretechingStatus();
	}

	private static final Allocation EMPTY_ALLOCATION = new Allocation(EMPTY_CONTAINER_LIST, Resources.createResource(0));

	@Override
	public Allocation allocate(ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask, List<ContainerId> release) {
		FiCaSchedulerApp application = getApplication(applicationAttemptId);
		if (application == null) {
			LOG.error("Calling allocate on removed " + "or non existant application " + applicationAttemptId);
			return EMPTY_ALLOCATION;
		}

		// Sanity check
		SchedulerUtils.normalizeRequests(ask, resourceCalculator, clusterResource, minimumAllocation);

		// Release containers
		for (ContainerId releasedContainer : release) {
			RMContainer rmContainer = getRMContainer(releasedContainer);
			if (rmContainer == null) {
				RMAuditLogger.logFailure(application.getUser(), AuditConstants.RELEASE_CONTAINER, "Unauthorized access or invalid container", "FifoScheduler",
						"Trying to release container not owned by app or with invalid id", application.getApplicationId(), releasedContainer);
			}
			LOG.error("AM release container: " + rmContainer.getContainerId());
			containerCompleted(rmContainer, SchedulerUtils.createAbnormalContainerStatus(releasedContainer, SchedulerUtils.RELEASED_CONTAINER), RMContainerEventType.RELEASED);
		}

		synchronized (application) {
			if (!ask.isEmpty()) {
				LOG.debug("allocate: pre-update" + " applicationId=" + applicationAttemptId + " application=" + application);
				application.showRequests();

				// Update application requests
				application.updateResourceRequests(ask);

				LOG.debug("allocate: post-update" + " applicationId=" + applicationAttemptId + " application=" + application);
				application.showRequests();

				LOG.debug("allocate:" + " applicationId=" + applicationAttemptId + " #ask=" + ask.size());
			}

			return new Allocation(application.pullNewlyAllocatedContainers(), application.getHeadroom());
		}
	}

	private FiCaSchedulerApp getApplication(ApplicationAttemptId applicationAttemptId) {
		return applications.get(applicationAttemptId);
	}

	@Override
	public SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId applicationAttemptId) {
		FiCaSchedulerApp app = getApplication(applicationAttemptId);
		return app == null ? null : new SchedulerAppReport(app);
	}

	private FiCaSchedulerNode getNode(NodeId nodeId) {
		return nodes.get(nodeId);
	}

	private synchronized void addApplication(ApplicationAttemptId appAttemptId, String user) {
		this.applicationSplitManager.createApplicationSplitRecord(appAttemptId.getApplicationId());
		// TODO: Fix store
		FiCaSchedulerApp schedulerApp = new FiCaSchedulerApp(appAttemptId, user, DEFAULT_QUEUE, activeUsersManager, this.rmContext);
		applications.put(appAttemptId, schedulerApp);
		metrics.submitApp(user, appAttemptId.getAttemptId());
		LOG.info("Application Submission: " + appAttemptId.getApplicationId() + " from " + user + ", currently active: " + applications.size());
		rmContext.getDispatcher().getEventHandler().handle(new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.APP_ACCEPTED));
	}

	private synchronized void doneApplication(ApplicationAttemptId applicationAttemptId, RMAppAttemptState rmAppAttemptFinalState) throws IOException {
		this.applicationSplitManager.removeApplicationSplitRecord(applicationAttemptId.getApplicationId());
		FiCaSchedulerApp application = getApplication(applicationAttemptId);
		if (application == null) {
			throw new IOException("Unknown application " + applicationAttemptId + " has completed!");
		}

		// Kill all 'live' containers
		for (RMContainer container : application.getLiveContainers()) {
			containerCompleted(container, SchedulerUtils.createAbnormalContainerStatus(container.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION), RMContainerEventType.KILL);
		}

		// Inform the activeUsersManager
		synchronized (application) {
			activeUsersManager.deactivateApplication(application.getUser(), application.getApplicationId());
		}

		// Clean up pending requests, metrics etc.
		application.stop(rmAppAttemptFinalState);

		// Remove the application
		applications.remove(applicationAttemptId);
	}

	/**
	 * Heart of the scheduler...
	 * 
	 * @param node
	 *            node on which resources are available to be allocated
	 */
	private void assignContainers(FiCaSchedulerNode node) {
		LOG.debug("assignContainers:" + " node=" + node.getRMNode().getNodeAddress() + " #applications=" + applications.size());

		// Try to assign containers to applications in fifo order
		for (Map.Entry<ApplicationAttemptId, FiCaSchedulerApp> e : applications.entrySet()) {
			FiCaSchedulerApp application = e.getValue();
			LOG.debug("pre-assignContainers");
			application.showRequests();
			synchronized (application) {
				for (Priority priority : application.getPriorities()) {
					int maxContainers = getMaxAllocatableContainers(application, priority, node, NodeType.OFF_SWITCH);
					LOG.error("@@ Scheduler: max allocatable containers=" + maxContainers + ", priority=" + priority);
					// Ensure the application needs containers of this priority
					if (maxContainers > 0) {
						int assignedContainers = assignContainersOnNode(node, application, priority);
						// Do not assign out of order w.r.t priorities
						if (assignedContainers == 0) {
							break;
						}
					}
				}
			}

			LOG.debug("post-assignContainers");
			application.showRequests();

			// Done
			if (Resources.lessThan(resourceCalculator, clusterResource, node.getAvailableResource(), minimumAllocation)) {
				break;
			}
		}

		// Update the applications' headroom to correctly take into
		// account the containers assigned in this update.
		for (FiCaSchedulerApp application : applications.values()) {
			application.setHeadroom(Resources.subtract(clusterResource, usedResource));
		}
	}

	private int getMaxAllocatableContainers(FiCaSchedulerApp application, Priority priority, FiCaSchedulerNode node, NodeType type) {
		ResourceRequest offSwitchRequest = application.getResourceRequest(priority, FiCaSchedulerNode.ANY);
		int maxContainers = offSwitchRequest.getNumContainers();

		if (type == NodeType.OFF_SWITCH) {
			return maxContainers;
		}

		if (type == NodeType.RACK_LOCAL) {
			ResourceRequest rackLocalRequest = application.getResourceRequest(priority, node.getRMNode().getRackName());
			if (rackLocalRequest == null) {
				return maxContainers;
			}

			maxContainers = Math.min(maxContainers, rackLocalRequest.getNumContainers());
		}

		if (type == NodeType.NODE_LOCAL) {
			ResourceRequest nodeLocalRequest = application.getResourceRequest(priority, node.getRMNode().getNodeAddress());
			if (nodeLocalRequest != null) {
				maxContainers = Math.min(maxContainers, nodeLocalRequest.getNumContainers());
			}
		}

		return maxContainers;
	}

	private int assignContainersOnNode(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority) {

		LOG.error("@@ Scheduler: assign containers on node=" + node.getNodeID() + ", slot=" + (node.getAvailableResource().getMemory() / 1024) + ", priority=" + priority);

		int nodePrefetchedContainers = 0;
		int nodeContainers = 0;
		int otherContainers = 0;

		// for map tasks (handling failed map tasks?)
		if (priority.getPriority() == 20 || priority.getPriority() == 5) {
			LOG.error("@@ Scheduler: before assign -> resource=" + node.getAvailableResource() + ", node=" + node.getNodeID());
			nodePrefetchedContainers = assignNodeByPrefetching(node, application, priority);
			LOG.error("@@ Scheduler: after assign -> resource=" + node.getAvailableResource() + ", node=" + node.getNodeID());
		} else {
			otherContainers = assignOffSwitchContainers(node, application, priority);
		}
		// int nodeContainers = assignNodeWithoutPrefetching(node, application,
		// priority);

		LOG.error("@@ Scheduler: hits=" + nodePrefetchedContainers + ", misses=" + nodeContainers + ", others=" + otherContainers);

		LOG.debug("assignContainersOnNode:" + " node=" + node.getRMNode().getNodeAddress() + " application=" + application.getApplicationId().getId() + " priority=" + priority.getPriority()
				+ " #assigned=" + (nodePrefetchedContainers + nodeContainers + otherContainers));

		return (nodePrefetchedContainers + nodeContainers + otherContainers);
	}

	private int assignNodeByPrefetching(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority) {

		LOG.error("@@ Scheduler: assign by prefetching -> priority=" + priority);
		NodeSplitContext nodeContext = this.inMemoryManager.lookupNode(node.getNodeID());
		ApplicationSplitConext applicationContext = this.applicationSplitManager.lookupApplication(application.getApplicationId());
		if (applicationContext.scheduleTaskQueue.size() > 0) {
			synchronized (applicationContext) {
				List<SplitContext> unscheduledTasks = applicationContext.scheduleTaskQueue;
				LOG.error("@@ Scheduler: unscheduled tasks: " + unscheduledTasks.size());
				// get request for map tasks without node host constraint
				ResourceRequest request = application.getResourceRequest(priority, FiCaSchedulerNode.ANY);
				// int assignableContainers = Math.min(scheduleSplitList.size(),
				// request.getNumContainers());
				int availableContainers = node.getAvailableResource().getMemory() / request.getCapability().getMemory();
				int assignableContainers = Math.min(unscheduledTasks.size(), availableContainers);
				LOG.error("@@ Scheduler: assignableContainers=" + assignableContainers);
				int assignedCount = 0;
				// TODO: what if split is not prefetched?
				Iterator<SplitContext> it = unscheduledTasks.iterator();
				for (; assignedCount < assignableContainers && it.hasNext();) {
					SplitContext splitContext = it.next();
					int assignedContainers = -1;
					if (nodeContext.isPrefetchedSplit(splitContext)) {
						assignedContainers = assignContainer(node, application, priority, 1, request, NodeType.OFF_SWITCH, splitContext);
						assignedCount += assignedContainers;
						if (assignedContainers == 1) {
							LOG.error("@@ Scheduler: assign => task=" + splitContext.pi.taskId + ", node=" + node.getNodeID());
							it.remove();
						}
						LOG.error("@@ Scheduler: prefetch hits -> node=" + node.getNodeID() + ", task=" + splitContext.pi.taskId + ", assigned=" + assignedContainers + ", node=" + node.getNodeID());
					}
					LOG.error("@@ Scheduler: node=" + node.getNodeID() + ", assigned=" + assignedContainers + ", assignedCount=" + assignedCount);
				}
				LOG.error("@@ Scheduler: schedule list=" + unscheduledTasks.size() + ", request list=" + request.getNumContainers() + ", prefetch list=" + unscheduledTasks.size() + ", assigned="
						+ availableContainers + ", slots=" + availableContainers + ", node=" + node.getNodeID());
				return assignedCount;
			}
		} else {
			LOG.error("@@ Scheduler: no prefetch splits on node=" + node.getNodeID());
		}

		return 0;

	}

	private int assignOffSwitchContainers(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority) {
		int assignedContainers = 0;
		ResourceRequest request = application.getResourceRequest(priority, FiCaSchedulerNode.ANY);
		if (request != null) {
			assignedContainers = assignContainer(node, application, priority, request.getNumContainers(), request, NodeType.OFF_SWITCH);
		}
		return assignedContainers;
	}

	private int assignContainer(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority, int assignableContainers, ResourceRequest request, NodeType type) {
		LOG.debug("assignContainers:" + " node=" + node.getRMNode().getNodeAddress() + " application=" + application.getApplicationId().getId() + " priority=" + priority.getPriority()
				+ " assignableContainers=" + assignableContainers + " request=" + request + " type=" + type);
		Resource capability = request.getCapability();

		int availableContainers = node.getAvailableResource().getMemory() / capability.getMemory();
		int assignedContainers = Math.min(assignableContainers, availableContainers);

		if (assignedContainers > 0) {
			for (int i = 0; i < assignedContainers; ++i) {

				NodeId nodeId = node.getRMNode().getNodeID();
				ContainerId containerId = BuilderUtils.newContainerId(application.getApplicationAttemptId(), application.getNewContainerId());
				ContainerToken containerToken = null;

				// If security is enabled, send the container-tokens too.
				if (UserGroupInformation.isSecurityEnabled()) {
					containerToken = this.rmContext.getContainerTokenSecretManager().createContainerToken(containerId, nodeId, application.getUser(), capability);
					if (containerToken == null) {
						return i; // Try again later.
					}
				}

				// Create the container
				Container container = BuilderUtils.newContainer(containerId, nodeId, node.getRMNode().getHttpAddress(), capability, priority, containerToken);

				// Allocate!

				// Inform the application
				RMContainer rmContainer = application.allocate(type, node, priority, request, container);

				// Inform the node
				node.allocateContainer(application.getApplicationId(), rmContainer);

				// Update usage for this container
				Resources.addTo(usedResource, capability);
			}

		}

		return assignedContainers;
	}

	private int assignContainer(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority, int assignableContainers, ResourceRequest request, NodeType type, SplitContext as) {
		LOG.debug("assignContainers:" + " node=" + node.getRMNode().getNodeAddress() + " application=" + application.getApplicationId().getId() + " priority=" + priority.getPriority()
				+ " assignableContainers=" + assignableContainers + " request=" + request + " type=" + type);
		Resource capability = request.getCapability();

		int availableContainers = node.getAvailableResource().getMemory() / capability.getMemory();
		int assignedContainers = Math.min(assignableContainers, availableContainers);

		if (assignedContainers > 0) {
			for (int i = 0; i < assignedContainers; ++i) {

				NodeId nodeId = node.getRMNode().getNodeID();
				ContainerId containerId = BuilderUtils.newContainerId(application.getApplicationAttemptId(), application.getNewContainerId());
				ContainerToken containerToken = null;

				// If security is enabled, send the container-tokens too.
				if (UserGroupInformation.isSecurityEnabled()) {
					containerToken = this.rmContext.getContainerTokenSecretManager().createContainerToken(containerId, nodeId, application.getUser(), capability);
					if (containerToken == null) {
						return i; // Try again later.
					}
				}

				// Create the container
				Container container = BuilderUtils.newContainer(containerId, nodeId, node.getRMNode().getHttpAddress(), capability, priority, containerToken);
				LOG.error("## Container=" + container);
				// Allocate!
				this.applicationSplitManager.assignApplicationSplit(application.getApplicationId(), as, container.getId());
				// Inform the application
				RMContainer rmContainer = application.allocate(type, node, priority, request, container);
				LOG.error("## RMContainer=" + rmContainer);

				// Inform the node
				node.allocateContainer(application.getApplicationId(), rmContainer);

				// Update usage for this container
				Resources.addTo(usedResource, capability);
			}

		}

		return assignedContainers;
	}

	private synchronized void nodeUpdate(RMNode rmNode, List<ContainerStatus> newlyLaunchedContainers, List<ContainerStatus> completedContainers) {
		FiCaSchedulerNode node = getNode(rmNode.getNodeID());

		// Processing the newly launched containers
		for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
			containerLaunchedOnNode(launchedContainer.getContainerId(), node);
		}

		// Process completed containers
		for (ContainerStatus completedContainer : completedContainers) {
			ContainerId containerId = completedContainer.getContainerId();
			LOG.error("Container FINISHED: " + containerId);
			containerCompleted(getRMContainer(containerId), completedContainer, RMContainerEventType.FINISHED);
			this.inMemoryManager.updatePrefetchWindow(rmNode.getNodeID());
		}

		LOG.error("@@ Scheduler: node update -> node=" + node.getNodeID() + ", slot=" + (node.getAvailableResource().getMemory() / 1024));

		if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource, node.getAvailableResource(), minimumAllocation)) {
			LOG.error("Node heartbeat " + rmNode.getNodeID() + " available resource = " + node.getAvailableResource());

			// assignContainers(node);

			LOG.debug("Node after allocation " + rmNode.getNodeID() + " resource = " + node.getAvailableResource());
		}

		metrics.setAvailableResourcesToQueue(Resources.subtract(clusterResource, usedResource));
	}

	@Override
	public void handle(SchedulerEvent event) {
		switch (event.getType()) {
		case NODE_ADDED: {
			NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
			addNode(nodeAddedEvent.getAddedRMNode());
			this.inMemoryManager.addNode(this.nodes.get(nodeAddedEvent.getAddedRMNode().getNodeID()));
		}
			break;
		case NODE_REMOVED: {
			NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent) event;
			removeNode(nodeRemovedEvent.getRemovedRMNode());
			this.inMemoryManager.removeNode(nodeRemovedEvent.getRemovedRMNode().getNodeID());
		}
			break;
		case NODE_UPDATE: {
			NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent) event;
			nodeUpdate(nodeUpdatedEvent.getRMNode(), nodeUpdatedEvent.getNewlyLaunchedContainers(), nodeUpdatedEvent.getCompletedContainers());
		}
			break;
		case APP_ADDED: {
			AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
			addApplication(appAddedEvent.getApplicationAttemptId(), appAddedEvent.getUser());
		}
			break;
		case APP_REMOVED: {
			AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent) event;
			try {
				doneApplication(appRemovedEvent.getApplicationAttemptID(), appRemovedEvent.getFinalAttemptState());
			} catch (IOException ie) {
				LOG.error("Unable to remove application " + appRemovedEvent.getApplicationAttemptID(), ie);
			}
		}
			break;
		case CONTAINER_EXPIRED: {
			ContainerExpiredSchedulerEvent containerExpiredEvent = (ContainerExpiredSchedulerEvent) event;
			ContainerId containerid = containerExpiredEvent.getContainerId();
			containerCompleted(getRMContainer(containerid), SchedulerUtils.createAbnormalContainerStatus(containerid, SchedulerUtils.EXPIRED_CONTAINER), RMContainerEventType.EXPIRE);
		}
			break;
		default:
			LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
		}
	}

	private void containerLaunchedOnNode(ContainerId containerId, FiCaSchedulerNode node) {
		// Get the application for the finished container
		ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
		FiCaSchedulerApp application = getApplication(applicationAttemptId);
		if (application == null) {
			LOG.info("Unknown application: " + applicationAttemptId + " launched container " + containerId + " on node: " + node);
			// Some unknown container sneaked into the system. Kill it.
			this.rmContext.getDispatcher().getEventHandler().handle(new RMNodeCleanContainerEvent(node.getNodeID(), containerId));

			return;
		}

		application.containerLaunchedOnNode(containerId, node.getNodeID());
	}

	@Lock(InMemoryScheduler.class)
	private synchronized void containerCompleted(RMContainer rmContainer, ContainerStatus containerStatus, RMContainerEventType event) {
		if (rmContainer == null) {
			LOG.info("Null container completed...");
			return;
		}

		// Get the application for the finished container
		Container container = rmContainer.getContainer();
		ApplicationAttemptId applicationAttemptId = container.getId().getApplicationAttemptId();
		FiCaSchedulerApp application = getApplication(applicationAttemptId);

		// Get the node on which the container was allocated
		FiCaSchedulerNode node = getNode(container.getNodeId());

		if (application == null) {
			LOG.info("Unknown application: " + applicationAttemptId + " released container " + container.getId() + " on node: " + node + " with event: " + event);
			return;
		}

		// Inform the application
		application.containerCompleted(rmContainer, containerStatus, event);

		// Inform the node
		node.releaseContainer(container);

		// Update total usage
		Resources.subtractFrom(usedResource, container.getResource());

		LOG.error("Application " + applicationAttemptId + " released container " + container.getId() + " on node: " + node + " with event: " + event);

	}

	private Resource clusterResource = recordFactory.newRecordInstance(Resource.class);
	private Resource usedResource = recordFactory.newRecordInstance(Resource.class);

	private synchronized void removeNode(RMNode nodeInfo) {
		FiCaSchedulerNode node = getNode(nodeInfo.getNodeID());
		if (node == null) {
			return;
		}
		// Kill running containers
		for (RMContainer container : node.getRunningContainers()) {
			containerCompleted(container, SchedulerUtils.createAbnormalContainerStatus(container.getContainerId(), SchedulerUtils.LOST_CONTAINER), RMContainerEventType.KILL);
		}

		// Remove the node
		this.nodes.remove(nodeInfo.getNodeID());

		// Update cluster metrics
		Resources.subtractFrom(clusterResource, node.getRMNode().getTotalCapability());
	}

	@Override
	public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues, boolean recursive) {
		return DEFAULT_QUEUE.getQueueInfo(false, false);
	}

	@Override
	public List<QueueUserACLInfo> getQueueUserAclInfo() {
		return DEFAULT_QUEUE.getQueueUserAclInfo(null);
	}

	private synchronized void addNode(RMNode nodeManager) {
		this.nodes.put(nodeManager.getNodeID(), new FiCaSchedulerNode(nodeManager));
		Resources.addTo(clusterResource, nodeManager.getTotalCapability());
	}

	@Override
	public void recover(RMState state) {
		// NOT IMPLEMENTED
	}

	@Override
	public synchronized SchedulerNodeReport getNodeReport(NodeId nodeId) {
		FiCaSchedulerNode node = getNode(nodeId);
		return node == null ? null : new SchedulerNodeReport(node);
	}

	private RMContainer getRMContainer(ContainerId containerId) {
		FiCaSchedulerApp application = getApplication(containerId.getApplicationAttemptId());
		return (application == null) ? null : application.getRMContainer(containerId);
	}

	@Override
	public QueueMetrics getRootQueueMetrics() {
		return DEFAULT_QUEUE.getMetrics();
	}

	class ApplicationSplitManager {

		// lookup table for pi
		Map<PrefetchInfo, SplitContext> prefetchSplitRecord = new HashMap<PrefetchInfo, SplitContext>();
		// lookup table for container id
		Map<ContainerId, SplitContext> containerRecord = new HashMap<ContainerId, SplitContext>();

		Map<ApplicationId, ApplicationSplitConext> applicationSplitRecord = new HashMap<ApplicationId, ApplicationSplitConext>();

		public void createApplicationSplitRecord(ApplicationId applicationId) {
			LOG.error("@@ Scheduler: add application=" + applicationId);
			ApplicationSplitConext asc = new ApplicationSplitConext(applicationId, prefetchBlockSize);
			applicationSplitRecord.put(applicationId, asc);
		}

		public void removeApplicationSplitRecord(ApplicationId applicationId) {
			LOG.error("@@ Scheduler: remove application=" + applicationId);
			applicationSplitRecord.remove(applicationId);
		}

		public void addApplicationSplitRecord(ApplicationId applicationId, PrefetchInfo pi) {
			ApplicationSplitConext applicationContext = lookupApplication(applicationId);
			SplitContext splitContext = applicationContext.createSplit(pi);
			applicationContext.addSplit(splitContext);
			prefetchSplitRecord.put(pi, splitContext);
			inMemoryManager.addSplitToPrefetchQueue(splitContext);
		}

		public ApplicationSplitConext lookupApplication(ApplicationId applicationId) {
			return applicationSplitRecord.get(applicationId);
		}

		private SplitContext lookupSplitContext(PrefetchInfo pi) {
			return prefetchSplitRecord.get(pi);
		}

		private SplitContext lookupSplitContext(ContainerId containerId) {
			return containerRecord.get(containerId);
		}

		public void assignApplicationSplit(ApplicationId applicationId, SplitContext splitContext, ContainerId containerId) {
			LOG.error("@@ AMS: assign split -> task=" + splitContext.pi.taskId + ", container=" + containerId);
			ApplicationSplitConext applicationContext = lookupApplication(applicationId);
			applicationContext.assignContainer(splitContext, containerId);
			containerRecord.put(containerId, splitContext);
		}

		public int getUnscheduledSplit() {
			int count = 0;
			for (ApplicationSplitConext context : applicationSplitRecord.values()) {
				count += context.scheduleTaskQueue.size();
			}
			return count;
		}

		public synchronized void printSplitList() {
			LOG.error("@@ AMS: print unschedule list>>>");
			for (ApplicationSplitConext context : applicationSplitRecord.values()) {
				LOG.error("@@ AMS: application=" + context.applicationId);
				for (SplitContext splitContext : context.scheduleTaskQueue) {
					try {
						LOG.error("@@ AMS: split=" + splitContext);
						for (NodeSplitContext nodeContext : inMemoryManager.nodeRecord.values()) {
							LOG.error("\t[" + nodeContext.isPrefetchedSplit(splitContext) + "] " + nodeContext.nodeId);
						}
					} catch (Exception e) {
						LOG.fatal("@_@", e);
					}
				}
			}
		}

	}

	class InMemoryManager {

		// node list
		Map<NodeId, NodeSplitContext> nodeRecord = new HashMap<NodeId, NodeSplitContext>();
		Map<SplitContext, NodeSplitContext> nodePrefetchRecord = new HashMap<SplitContext, NodeSplitContext>();
		LinkedList<SplitContext> prefetchQueue = new LinkedList<SplitContext>();

		private NodeSplitContext lookupNode(NodeId nodeId) {
			return nodeRecord.get(nodeId);
		}

		public void printPretechingStatus() {
			for (NodeSplitContext nodeContext : nodeRecord.values()) {
				LOG.error("@@ IMM: node=" + nodeContext.nodeId + ", prefetching=" + nodeContext.prefetchingTaskRecord.size() + ", available=" + nodeContext.availablePrefetchWindow + ", resource="
						+ (nodeContext.node.getAvailableResource().getMemory()));
			}
		}

		public void addNode(FiCaSchedulerNode node) {
			LOG.error("@@ IMM: add node -> node=" + node.getNodeID());
			NodeSplitContext nodeContext = new NodeSplitContext(node, prefetchWindow);
			nodeRecord.put(node.getNodeID(), nodeContext);
		}

		public void removeNode(NodeId nodeId) {
			LOG.error("@@ IMM: remove node -> node=" + nodeId);
			nodeRecord.remove(nodeId);
		}

		public void assignPrefetchTaskToNode(SplitContext splitContext, NodeSplitContext nodeContext) {
			this.nodePrefetchRecord.put(splitContext, nodeContext);
			splitContext.assignPrefetchNode(nodeContext);
			nodeContext.addPrefetchingTask(splitContext);
		}

		public void addSplitToPrefetchQueue(SplitContext splitContext) {
			prefetchQueue.add(splitContext);
			LOG.error("@@ IMM: prefetch queue=" + prefetchQueue.size());
		}

		public void updatePrefetchProgress(SplitContext as, int prefetchBytes) {
			LOG.error("@@ IMM: update prefetch progress -> " + as.pi.taskId + ", progress=" + prefetchBytes);
			as.prefetchedBytes = prefetchBytes;
			if (as.prefetchedBytes == (64 * 1024 * 1024)) {
				as.setPrefetchFinished();
			} else {
				LOG.error("@@ IMM: should not happen");
			}
		}

		public void updatePrefetchWindow(NodeId nodeId, int prefetchWindow) {
			LOG.error("@@ IMM: update prefetch window -> node=" + nodeId + ", window=" + prefetchWindow + ", prefetch=" + this.prefetchQueue.size());
			NodeSplitContext nodeContext = lookupNode(nodeId);
			// nodeContext.availablePrefetchWindow = prefetchWindow;
			askPrefetchTasks(nodeContext);
		}

		public void updatePrefetchWindow(NodeId nodeId) {
			NodeSplitContext nodeContext = lookupNode(nodeId);
			nodeContext.availablePrefetchWindow++;
			LOG.error("@@ IMM: update prefetch window -> node=" + nodeId + ", window=" + nodeContext.availablePrefetchWindow + ", prefetch=" + this.prefetchQueue.size());
		}

		public void askPrefetchTasks(NodeSplitContext nodeContext) {
			synchronized (prefetchQueue) {
				Iterator<SplitContext> iterator = this.prefetchQueue.iterator();
				while (iterator.hasNext() && nodeContext.hasAvailablePrefetchWindow()) {
					SplitContext splitContext = iterator.next();
					LOG.error("@@ IMM: add prefetching task -> task=" + splitContext.pi.taskId + ", node=" + nodeContext.nodeId);
					assignPrefetchTaskToNode(splitContext, nodeContext);
					iterator.remove();
				}
			}
		}

		public void revokePrefetchSplit(SplitContext splitContext) {
			NodeSplitContext nodeContext = nodePrefetchRecord.get(splitContext);
			nodeContext.revokTask(splitContext);
		}

	}

}

class SplitContext {

	// basic
	public PrefetchInfo pi;
	public ApplicationSplitConext applicationContext;
	public long totalBytes = -1;

	// prefetching
	public NodeSplitContext nodeContext;
	public long prefetchedBytes = -1;
	public boolean prefetchStarted = false;
	public boolean prefetchFinished = false;

	// scheduling
	public ContainerId containerId;
	public boolean taskCompleted = false;

	public SplitContext(PrefetchInfo pi, ApplicationSplitConext applicationContext, long totalBytes) {
		this.pi = pi;
		this.applicationContext = applicationContext;
		this.totalBytes = totalBytes;
	}

	public boolean isPrefetchStarted() {
		return prefetchStarted;
	};

	public void setPrefetchStarted() {
		this.prefetchStarted = true;
	}

	public boolean isPrefetchFinished() {
		return prefetchFinished;
	}

	public void setPrefetchFinished() {
		this.prefetchFinished = true;
	}

	public boolean isTaskCompleted() {
		return taskCompleted;
	}

	public void setTaskCompleted() {
		this.taskCompleted = true;
	}

	// assigned for prefetch
	public boolean isPrefetchAssigned() {
		return nodeContext != null;
	}

	// scheduled to execute
	public boolean isTaskScheduled() {
		return containerId != null;
	}

	public void assignPrefetchNode(NodeSplitContext nodeContext) {
		this.nodeContext = nodeContext;
	}

	public void assignContainer(ContainerId cId) {
		this.containerId = cId;
		this.pi.containerId = cId.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((applicationContext == null) ? 0 : applicationContext.hashCode());
		result = prime * result + ((pi == null) ? 0 : pi.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SplitContext other = (SplitContext) obj;
		if (applicationContext == null) {
			if (other.applicationContext != null)
				return false;
		} else if (!applicationContext.equals(other.applicationContext))
			return false;
		if (pi == null) {
			if (other.pi != null)
				return false;
		} else if (!pi.equals(other.pi))
			return false;
		return true;
	}

}

class ApplicationSplitConext {

	ApplicationId applicationId;
	Set<SplitContext> splitRecord = new HashSet<SplitContext>();

	LinkedList<SplitContext> assignedTaskQueue = new LinkedList<SplitContext>();
	LinkedList<SplitContext> scheduleTaskQueue = new LinkedList<SplitContext>();
	long blockSize;

	public ApplicationSplitConext(ApplicationId applicationId, long blockSize) {
		this.applicationId = applicationId;
		this.blockSize = blockSize;
	}

	public SplitContext createSplit(PrefetchInfo pi) {
		SplitContext as = new SplitContext(pi, this, this.blockSize);
		return as;
	}

	public void addSplit(SplitContext split) {
		splitRecord.add(split);
		scheduleTaskQueue.add(split);
	}

	public List<SplitContext> pullAssignedTasks() {
		List<SplitContext> assignedTasks = new LinkedList<SplitContext>(assignedTaskQueue);
		assignedTaskQueue.clear();
		return assignedTasks;
	}

	public void assignContainer(SplitContext splitContext, ContainerId containerId) {
		splitContext.assignContainer(containerId);
		assignedTaskQueue.add(splitContext);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((applicationId == null) ? 0 : applicationId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ApplicationSplitConext other = (ApplicationSplitConext) obj;
		if (applicationId == null) {
			if (other.applicationId != null)
				return false;
		} else if (!applicationId.equals(other.applicationId))
			return false;
		return true;
	}

}

class NodeSplitContext {

	NodeId nodeId;
	FiCaSchedulerNode node;
	int availablePrefetchWindow;

	Set<SplitContext> prefetchingTaskRecord = new HashSet<SplitContext>();

	LinkedList<SplitContext> pendingTaskQueue = new LinkedList<SplitContext>();
	LinkedList<SplitContext> revokedTaskQueue = new LinkedList<SplitContext>();

	public NodeSplitContext(FiCaSchedulerNode node, int prefetchWindow) {
		this.node = node;
		this.nodeId = node.getNodeID();
		this.availablePrefetchWindow = prefetchWindow;
	}

	public void addPrefetchingTask(SplitContext splitContext) {
		prefetchingTaskRecord.add(splitContext);
		pendingTaskQueue.add(splitContext);
		this.availablePrefetchWindow--;
	}

	public boolean hasAvailablePrefetchWindow() {
		return this.availablePrefetchWindow > 0;
	}

	public List<SplitContext> pullPrefetchingTasks() {
		List<SplitContext> prefetchingTasks = new LinkedList<SplitContext>(pendingTaskQueue);
		pendingTaskQueue.clear();
		return prefetchingTasks;
	}

	public boolean isPrefetchedSplit(SplitContext splitContext) {
		return prefetchingTaskRecord.contains(splitContext);
	}

	public void revokTask(SplitContext splitContext) {
		this.prefetchingTaskRecord.remove(splitContext);
		this.revokedTaskQueue.add(splitContext);
	}

	public List<SplitContext> pullCompletedPrefetchTask() {
		List<SplitContext> revokedPrefetchTaskList = new LinkedList<SplitContext>(this.revokedTaskQueue);
		this.revokedTaskQueue.clear();
		return revokedPrefetchTaskList;
	}

}
