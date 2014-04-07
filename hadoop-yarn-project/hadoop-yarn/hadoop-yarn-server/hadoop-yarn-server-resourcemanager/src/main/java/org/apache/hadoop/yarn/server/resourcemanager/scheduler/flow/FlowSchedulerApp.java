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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowSchedulerTask.Type;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * Represents an Application from the viewpoint of the scheduler. Each running
 * Application in the RM corresponds to one instance of this class.
 */
@SuppressWarnings("unchecked")
@Private
@Unstable
public class FlowSchedulerApp extends SchedulerApplication {

	private static final Log LOG = LogFactory.getLog(FlowSchedulerApp.class);

	private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

	private final AppSchedulingInfo appSchedulingInfo;
	private final Queue queue;

	private final Resource currentConsumption = recordFactory.newRecordInstance(Resource.class);
	private Resource resourceLimit = recordFactory.newRecordInstance(Resource.class);

	private Map<ContainerId, RMContainer> liveContainers = new HashMap<ContainerId, RMContainer>();
	private List<RMContainer> newlyAllocatedContainers = new ArrayList<RMContainer>();

	final Map<Priority, Map<NodeId, RMContainer>> reservedContainers = new HashMap<Priority, Map<NodeId, RMContainer>>();

	/**
	 * Count how many times the application has been given an opportunity to
	 * schedule a task at each priority. Each time the scheduler asks the
	 * application for a task at this priority, it is incremented, and each time
	 * the application successfully schedules a task, it is reset to 0.
	 */
	Multiset<Priority> schedulingOpportunities = HashMultiset.create();

	Multiset<Priority> reReservations = HashMultiset.create();

	Resource currentReservation = recordFactory.newRecordInstance(Resource.class);

	private final RMContext rmContext;

	// my enhancement
	FlowSchedulerTask appMaster;
	ContainerId appMasterContainer;
	Map<FlowSchedulerTask, ContainerId> mapTasks = new HashMap<FlowSchedulerTask, ContainerId>();
	Map<FlowSchedulerTask, ContainerId> reduceTasks = new HashMap<FlowSchedulerTask, ContainerId>();
	List<FlowSchedulerTask> launchedTasks = new LinkedList<FlowSchedulerTask>();
	// taskAttemptId -> task
	Map<String, FlowSchedulerTask> createdTaskMap = new HashMap<String, FlowSchedulerTask>();
	FlowSchedulerManager flowSchedulerManager;

	public FlowSchedulerApp(ApplicationAttemptId applicationAttemptId, String user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext, FlowSchedulerManager flowSchedulerManager) {
		this.rmContext = rmContext;
		this.appSchedulingInfo = new AppSchedulingInfo(applicationAttemptId, user, queue, activeUsersManager);
		this.queue = queue;
		this.flowSchedulerManager = flowSchedulerManager;
	}

	public String getApplicationName() {
		return rmContext.getRMApps().get(getApplicationId()).getName();
	}

	public ApplicationId getApplicationId() {
		return this.appSchedulingInfo.getApplicationId();
	}

	@Override
	public ApplicationAttemptId getApplicationAttemptId() {
		return this.appSchedulingInfo.getApplicationAttemptId();
	}

	public String getUser() {
		return this.appSchedulingInfo.getUser();
	}

	public Map<String, ResourceRequest> getResourceRequests(Priority priority) {
		return this.appSchedulingInfo.getResourceRequests(priority);
	}

	public int getNewContainerId() {
		return this.appSchedulingInfo.getNewContainerId();
	}

	public Collection<Priority> getPriorities() {
		return this.appSchedulingInfo.getPriorities();
	}

	public ResourceRequest getResourceRequest(Priority priority, String nodeAddress) {
		return this.appSchedulingInfo.getResourceRequest(priority, nodeAddress);
	}

	public synchronized int getTotalRequiredResources(Priority priority) {
		return getResourceRequest(priority, RMNode.ANY).getNumContainers();
	}

	public Resource getResource(Priority priority) {
		return this.appSchedulingInfo.getResource(priority);
	}

	/**
	 * Is this application pending?
	 * 
	 * @return true if it is else false.
	 */
	@Override
	public boolean isPending() {
		return this.appSchedulingInfo.isPending();
	}

	public String getQueueName() {
		return this.appSchedulingInfo.getQueueName();
	}

	/**
	 * Get the list of live containers
	 * 
	 * @return All of the live containers
	 */
	@Override
	public synchronized Collection<RMContainer> getLiveContainers() {
		return new ArrayList<RMContainer>(liveContainers.values());
	}

	public synchronized void stop(RMAppAttemptState rmAppAttemptFinalState) {
		// Cleanup all scheduling information
		this.appSchedulingInfo.stop(rmAppAttemptFinalState);
	}

	public synchronized void containerLaunchedOnNode(ContainerId containerId, NodeId nodeId) {
		// Inform the container
		RMContainer rmContainer = getRMContainer(containerId);
		if (rmContainer == null) {
			// Some unknown container sneaked into the system. Kill it.
			this.rmContext.getDispatcher().getEventHandler().handle(new RMNodeCleanContainerEvent(nodeId, containerId));
			return;
		}

		rmContainer.handle(new RMContainerEvent(containerId, RMContainerEventType.LAUNCHED));
	}

	synchronized public void containerCompleted(RMContainer rmContainer, ContainerStatus containerStatus, RMContainerEventType event) {

		Container container = rmContainer.getContainer();
		ContainerId containerId = container.getId();

		// Inform the container
		rmContainer.handle(new RMContainerFinishedEvent(containerId, containerStatus, event));
		LOG.info("Completed container: " + rmContainer.getContainerId() + " in state: " + rmContainer.getState() + " event:" + event);

		// Remove from the list of containers
		liveContainers.remove(rmContainer.getContainerId());

		RMAuditLogger.logSuccess(getUser(), AuditConstants.RELEASE_CONTAINER, "SchedulerApp", getApplicationId(), containerId);

		// Update usage metrics
		Resource containerResource = rmContainer.getContainer().getResource();
		queue.getMetrics().releaseResources(getUser(), 1, containerResource);
		Resources.subtractFrom(currentConsumption, containerResource);
	}

	synchronized public RMContainer allocate(NodeType type, FlowSchedulerNode node, Priority priority, ResourceRequest request, Container container) {

		// Required sanity check - AM can call 'allocate' to update resource
		// request without locking the scheduler, hence we need to check
		if (getTotalRequiredResources(priority) <= 0) {
			return null;
		}

		// Create RMContainer
		RMContainer rmContainer = new RMContainerImpl(container, this.getApplicationAttemptId(), node.getNodeID(), this.rmContext.getDispatcher().getEventHandler(),
				this.rmContext.getContainerAllocationExpirer());

		// Add it to allContainers list.
		newlyAllocatedContainers.add(rmContainer);
		liveContainers.put(container.getId(), rmContainer);

		// Update consumption and track allocations
		appSchedulingInfo.allocate(type, node, priority, request, container);
		Resources.addTo(currentConsumption, container.getResource());

		// Inform the container
		rmContainer.handle(new RMContainerEvent(container.getId(), RMContainerEventType.START));

		if (LOG.isDebugEnabled()) {
			LOG.debug("allocate: applicationAttemptId=" + container.getId().getApplicationAttemptId() + " container=" + container.getId() + " host=" + container.getNodeId().getHost() + " type="
					+ type);
		}
		RMAuditLogger.logSuccess(getUser(), AuditConstants.ALLOC_CONTAINER, "SchedulerApp", getApplicationId(), container.getId());

		return rmContainer;
	}

	synchronized public List<Container> pullNewlyAllocatedContainers() {
		List<Container> returnContainerList = new ArrayList<Container>(newlyAllocatedContainers.size());
		for (RMContainer rmContainer : newlyAllocatedContainers) {
			rmContainer.handle(new RMContainerEvent(rmContainer.getContainerId(), RMContainerEventType.ACQUIRED));
			returnContainerList.add(rmContainer.getContainer());
		}
		newlyAllocatedContainers.clear();
		return returnContainerList;
	}

	public Resource getCurrentConsumption() {
		return this.currentConsumption;
	}

	public synchronized RMContainer getRMContainer(ContainerId id) {
		return liveContainers.get(id);
	}

	synchronized public void resetSchedulingOpportunities(Priority priority) {
		this.schedulingOpportunities.setCount(priority, 0);
	}

	synchronized public void addSchedulingOpportunity(Priority priority) {
		this.schedulingOpportunities.setCount(priority, schedulingOpportunities.count(priority) + 1);
	}

	/**
	 * Return the number of times the application has been given an opportunity
	 * to schedule a task at the given priority since the last time it
	 * successfully did so.
	 */
	synchronized public int getSchedulingOpportunities(Priority priority) {
		return this.schedulingOpportunities.count(priority);
	}

	/**
	 * Get the list of reserved containers
	 * 
	 * @return All of the reserved containers.
	 */
	@Override
	public synchronized List<RMContainer> getReservedContainers() {
		List<RMContainer> reservedContainers = new ArrayList<RMContainer>();
		for (Map.Entry<Priority, Map<NodeId, RMContainer>> e : this.reservedContainers.entrySet()) {
			reservedContainers.addAll(e.getValue().values());
		}
		return reservedContainers;
	}

	public synchronized void setHeadroom(Resource globalLimit) {
		this.resourceLimit = globalLimit;
	}

	/**
	 * Get available headroom in terms of resources for the application's user.
	 * 
	 * @return available resource headroom
	 */
	public synchronized Resource getHeadroom() {
		// Corner case to deal with applications being slightly over-limit
		if (resourceLimit.getMemory() < 0) {
			resourceLimit.setMemory(0);
		}

		return resourceLimit;
	}

	public Queue getQueue() {
		return queue;
	}

	// --------------------------------------

	public boolean isAppMasterScheduled() {
		return this.appMaster.isLaunched();
	}

	public List<FlowSchedulerTask> getMapTasks() {
		return new LinkedList<FlowSchedulerTask>(mapTasks.keySet());
	}

	public List<FlowSchedulerTask> getUnscheduledTasks() {
		List<FlowSchedulerTask> unscheduledTasks = new LinkedList<FlowSchedulerTask>();
		if (!isAppMasterScheduled()) {
			unscheduledTasks.add(getAppMasterTask());
		}
		unscheduledTasks.addAll(getUnscheduledMapTasks());
		unscheduledTasks.addAll(getUnscheduledReduceTasks());
		return unscheduledTasks;
	}

	public List<FlowSchedulerTask> getCompletedMapTasks() {
		List<FlowSchedulerTask> completedTasks = new LinkedList<FlowSchedulerTask>();
		for (FlowSchedulerTask task : mapTasks.keySet()) {
			if (task.isCompleted()) {
				completedTasks.add(task);
			}
		}
		return completedTasks;
	}

	public List<FlowSchedulerTask> getUnscheduledMapTasks() {
		List<FlowSchedulerTask> unscheduledTasks = new LinkedList<FlowSchedulerTask>();
		for (FlowSchedulerTask task : mapTasks.keySet()) {
			if (!task.isLaunched()) {
				unscheduledTasks.add(task);
			}
		}
		return unscheduledTasks;
	}

	public int getNumOfMapTasks() {
		return mapTasks.size();
	}

	public int getNumOfCompletedMapTasks() {
		int count = 0;
		for (FlowSchedulerTask task : mapTasks.keySet()) {
			if (task.isCompleted()) {
				count++;
			}
		}
		return count;
	}

	public int getNumOfLaunchedMapTasks() {
		int count = 0;
		for (FlowSchedulerTask task : mapTasks.keySet()) {
			if (task.isLaunched()) {
				count++;
			}
		}
		return count;
	}

	public int getNumOfUnscheduledMapTasks() {
		int count = 0;
		for (FlowSchedulerTask task : mapTasks.keySet()) {
			if (task.isUnscheduled()) {
				count++;
			}
		}
		return count;
	}

	public List<FlowSchedulerTask> getReduceTasks() {
		return new LinkedList<FlowSchedulerTask>(reduceTasks.keySet());
	}

	public List<FlowSchedulerTask> getUnscheduledReduceTasks() {
		List<FlowSchedulerTask> unscheduledTasks = new LinkedList<FlowSchedulerTask>();
		for (FlowSchedulerTask task : reduceTasks.keySet()) {
			if (!task.isLaunched()) {
				unscheduledTasks.add(task);
			}
		}
		return unscheduledTasks;
	}

	public int getNumOfReduceTasks() {
		return reduceTasks.size();
	}

	public int getNumOfCompletedReduceTasks() {
		int count = 0;
		for (FlowSchedulerTask task : reduceTasks.keySet()) {
			if (task.isCompleted()) {
				count++;
			}
		}
		return count;
	}

	public int getNumOfLaunchedReduceTasks() {
		int count = 0;
		for (FlowSchedulerTask task : reduceTasks.keySet()) {
			if (task.isLaunched()) {
				count++;
			}
		}
		return count;
	}

	public int getNumOfUnscheduledReduceTasks() {
		int count = 0;
		for (FlowSchedulerTask task : reduceTasks.keySet()) {
			if (task.isUnscheduled()) {
				count++;
			}
		}
		return count;
	}

	public FlowSchedulerTask getAppMasterTask() {
		return appMaster;
	}

	public boolean hasUnlaunchedTasks() {
		if (!appMaster.isLaunched()) {
			return true;
		}
		for (FlowSchedulerTask task : mapTasks.keySet()) {
			if (!task.isLaunched()) {
				return true;
			}
		}
		for (FlowSchedulerTask task : reduceTasks.keySet()) {
			if (!task.isLaunched()) {
				return true;
			}
		}
		return false;
	}

	public boolean isLaunched() {
		return appMasterContainer != null;
	}

	public void launchTask(FlowSchedulerTask task, FlowSchedulerNode node, RMContainer rmContainer) {
		task.launch(node, rmContainer);
	}

	public void completeTask(FlowSchedulerTask task, ContainerStatus containerStatus, RMContainerEventType event) {
		task.complete();
		containerCompleted(task.getRmContainer(), containerStatus, event);
	}

	public boolean isTaskCreated(String taskAttemptId) {
		return createdTaskMap.containsKey(taskAttemptId);
	}

	public void addTask(String taskId, String taskAttemptId, Type type, Priority priority, Resource capability, String host) {
		FlowRate flowRate = flowSchedulerManager.lookupFlowRate(this, type);
		FlowSchedulerStorage storage = flowSchedulerManager.lookupStorage(host);
		FlowSchedulerTask task = new FlowSchedulerTask(taskId, taskAttemptId, type, priority, flowRate, capability, this, storage);
		if (task.isAppMaster()) {
			this.appMaster = task;
		} else if (task.isMap()) {
			this.mapTasks.put(task, null);
		} else if (task.isReduce()) {
			this.reduceTasks.put(task, null);
		}
		createdTaskMap.put(taskAttemptId, task);
	}

	// Key method to integrate RM protocol to my own implementation
	public synchronized void updateResourceRequests(List<ResourceRequest> ask) {
		LOG.fatal("[Ask] update request with size=" + ask.size());
		Random rand = new Random();
		this.appSchedulingInfo.updateResourceRequests(ask);
		boolean containRequestDetail = false;
		for (ResourceRequest request : ask) {
			if (request.getRequestDetail() != null && request.getRequestDetail().length() > 0) {
				containRequestDetail = true;
				LOG.fatal("[Ask] request detail=" + request.getRequestDetail());
				String requestDetail = request.getRequestDetail();
				String[] taskAttempDetails = requestDetail.split(";");
				for (String taskAttempDetail : taskAttempDetails) {
					String[] detail = taskAttempDetail.split(":");
					String taskId = detail[0];
					String taskAttemptId = detail[1];
					String taskType = detail[2];
					String priorityString = detail[3];
					String dataHost = null;
					if (detail.length > 4) {
						String dataHosts = detail[4];
						String[] dataHostList = dataHosts.split(",");
						if (dataHostList.length > 0) {
							dataHost = dataHostList[rand.nextInt(dataHostList.length)];
						}
					}
					Type type = FlowSchedulerTask.getType(taskType);
					Priority priority = recordFactory.newRecordInstance(Priority.class);
					priority.setPriority(Integer.parseInt(priorityString));
					if (!isTaskCreated(taskAttemptId)) {
						addTask(taskId, taskAttemptId, type, priority, request.getCapability(), dataHost);
					} else {
						LOG.fatal("The taskAttempId exists: " + taskAttemptId);
					}
				}
			}
		}
		if (!containRequestDetail) {
			if (this.appMaster == null) {
				LOG.fatal("[Ask] no request detail...can be the AppMaster");
				Priority priority = recordFactory.newRecordInstance(Priority.class);
				priority.setPriority(0);
				Resource capability = ask.get(0).getCapability();
				addTask("AppMaster_" + this.getApplicationId(), "AppMaster_" + this.getApplicationAttemptId(), Type.AppMaster, priority, capability, null);
			}
		}
	}

}
