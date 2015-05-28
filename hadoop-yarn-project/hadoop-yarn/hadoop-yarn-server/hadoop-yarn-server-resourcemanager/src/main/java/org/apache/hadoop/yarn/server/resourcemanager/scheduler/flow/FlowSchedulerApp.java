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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
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
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityHeadroomProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowSchedulerTask.Type;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;

/**
 * Represents an application attempt from the viewpoint of the FIFO or Capacity
 * scheduler.
 */
@Private
@Unstable
public class FlowSchedulerApp extends SchedulerApplicationAttempt {

  private static final Log LOG = LogFactory.getLog(FlowSchedulerApp.class);

  private final Set<ContainerId> containersToPreempt =
    new HashSet<ContainerId>();
    
  private CapacityHeadroomProvider headroomProvider;

  public FlowSchedulerApp(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext, FlowSchedulerManager flowSchedulerManager) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
    
    RMApp rmApp = rmContext.getRMApps().get(getApplicationId());
    
    Resource amResource;
    if (rmApp == null || rmApp.getAMResourceRequest() == null) {
      //the rmApp may be undefined (the resource manager checks for this too)
      //and unmanaged applications do not provide an amResource request
      //in these cases, provide a default using the scheduler
      amResource = rmContext.getScheduler().getMinimumResourceCapability();
    } else {
      amResource = rmApp.getAMResourceRequest().getCapability();
    }
    
    setAMResource(amResource);
    
    this.flowSchedulerManager = flowSchedulerManager;
  }

  synchronized public boolean containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {

    // Remove from the list of containers
    if (null == liveContainers.remove(rmContainer.getContainerId())) {
      return false;
    }
    
    // Remove from the list of newly allocated containers if found
    newlyAllocatedContainers.remove(rmContainer);

    Container container = rmContainer.getContainer();
    ContainerId containerId = container.getId();

    // Inform the container
    rmContainer.handle(
        new RMContainerFinishedEvent(
            containerId,
            containerStatus, 
            event)
        );
    LOG.info("Completed container: " + rmContainer.getContainerId() + 
        " in state: " + rmContainer.getState() + " event:" + event);

    containersToPreempt.remove(rmContainer.getContainerId());

    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.RELEASE_CONTAINER, "SchedulerApp", 
        getApplicationId(), containerId);
    
    // Update usage metrics 
    Resource containerResource = rmContainer.getContainer().getResource();
    queue.getMetrics().releaseResources(getUser(), 1, containerResource);
    Resources.subtractFrom(currentConsumption, containerResource);

    // Clear resource utilization metrics cache.
    lastMemoryAggregateAllocationUpdateTime = -1;

    return true;
  }

  synchronized public RMContainer allocate(NodeType type, FlowSchedulerNode node,
      Priority priority, ResourceRequest request, 
      Container container) {

    if (isStopped) {
      return null;
    }
    
    // Required sanity check - AM can call 'allocate' to update resource 
    // request without locking the scheduler, hence we need to check
    if (getTotalRequiredResources(priority) <= 0) {
      return null;
    }
    
    // Create RMContainer
    RMContainer rmContainer = new RMContainerImpl(container, this
        .getApplicationAttemptId(), node.getNodeID(),
        appSchedulingInfo.getUser(), this.rmContext);

    // Add it to allContainers list.
    newlyAllocatedContainers.add(rmContainer);
    liveContainers.put(container.getId(), rmContainer);    

    // Update consumption and track allocations
    List<ResourceRequest> resourceRequestList = appSchedulingInfo.allocate(
        type, node, priority, request, container);
    Resources.addTo(currentConsumption, container.getResource());
    
    // Update resource requests related to "request" and store in RMContainer 
    ((RMContainerImpl)rmContainer).setResourceRequests(resourceRequestList);

    // Inform the container
    rmContainer.handle(
        new RMContainerEvent(container.getId(), RMContainerEventType.START));

    if (LOG.isDebugEnabled()) {
      LOG.debug("allocate: applicationAttemptId=" 
          + container.getId().getApplicationAttemptId() 
          + " container=" + container.getId() + " host="
          + container.getNodeId().getHost() + " type=" + type);
    }
    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.ALLOC_CONTAINER, "SchedulerApp", 
        getApplicationId(), container.getId());
    
    return rmContainer;
  }

  public synchronized boolean unreserve(FlowSchedulerNode node, Priority priority) {
    Map<NodeId, RMContainer> reservedContainers =
      this.reservedContainers.get(priority);

    if (reservedContainers != null) {
      RMContainer reservedContainer = reservedContainers.remove(node.getNodeID());

      // unreserve is now triggered in new scenarios (preemption)
      // as a consequence reservedcontainer might be null, adding NP-checks
      if (reservedContainer != null
          && reservedContainer.getContainer() != null
          && reservedContainer.getContainer().getResource() != null) {

        if (reservedContainers.isEmpty()) {
          this.reservedContainers.remove(priority);
        }
        // Reset the re-reservation count
        resetReReservations(priority);

        Resource resource = reservedContainer.getContainer().getResource();
        Resources.subtractFrom(currentReservation, resource);

        LOG.info("Application " + getApplicationId() + " unreserved "
            + " on node " + node + ", currently has " + reservedContainers.size()
            + " at priority " + priority + "; currentReservation "
            + currentReservation);
        return true;
      }
    }
    return false;
  }

  public synchronized float getLocalityWaitFactor(
      Priority priority, int clusterNodes) {
    // Estimate: Required unique resources (i.e. hosts + racks)
    int requiredResources = 
        Math.max(this.getResourceRequests(priority).size() - 1, 0);
    
    // waitFactor can't be more than '1' 
    // i.e. no point skipping more than clustersize opportunities
    return Math.min(((float)requiredResources / clusterNodes), 1.0f);
  }

  public synchronized Resource getTotalPendingRequests() {
    Resource ret = Resource.newInstance(0, 0);
    for (ResourceRequest rr : appSchedulingInfo.getAllResourceRequests()) {
      // to avoid double counting we count only "ANY" resource requests
      if (ResourceRequest.isAnyLocation(rr.getResourceName())){
        Resources.addTo(ret,
            Resources.multiply(rr.getCapability(), rr.getNumContainers()));
      }
    }
    return ret;
  }

  public synchronized void addPreemptContainer(ContainerId cont){
    // ignore already completed containers
    if (liveContainers.containsKey(cont)) {
      containersToPreempt.add(cont);
    }
  }

  /**
   * This method produces an Allocation that includes the current view
   * of the resources that will be allocated to and preempted from this
   * application.
   *
   * @param rc
   * @param clusterResource
   * @param minimumAllocation
   * @return an allocation
   */
  public synchronized Allocation getAllocation(ResourceCalculator rc,
      Resource clusterResource, Resource minimumAllocation) {

    Set<ContainerId> currentContPreemption = Collections.unmodifiableSet(
        new HashSet<ContainerId>(containersToPreempt));
    containersToPreempt.clear();
    Resource tot = Resource.newInstance(0, 0);
    for(ContainerId c : currentContPreemption){
      Resources.addTo(tot,
          liveContainers.get(c).getContainer().getResource());
    }
    int numCont = (int) Math.ceil(
        Resources.divide(rc, clusterResource, tot, minimumAllocation));
    ResourceRequest rr = ResourceRequest.newInstance(
        Priority.UNDEFINED, ResourceRequest.ANY,
        minimumAllocation, numCont);
    ContainersAndNMTokensAllocation allocation =
        pullNewlyAllocatedContainersAndNMTokens();
    Resource headroom = getHeadroom();
    setApplicationHeadroomForMetrics(headroom);
    return new Allocation(allocation.getContainerList(), headroom, null,
      currentContPreemption, Collections.singletonList(rr),
      allocation.getNMTokenList());
  }
  
  synchronized public NodeId getNodeIdToUnreserve(Priority priority,
      Resource resourceNeedUnreserve, ResourceCalculator rc,
      Resource clusterResource) {

    // first go around make this algorithm simple and just grab first
    // reservation that has enough resources
    Map<NodeId, RMContainer> reservedContainers = this.reservedContainers
        .get(priority);

    if ((reservedContainers != null) && (!reservedContainers.isEmpty())) {
      for (Map.Entry<NodeId, RMContainer> entry : reservedContainers.entrySet()) {
        NodeId nodeId = entry.getKey();
        Resource containerResource = entry.getValue().getContainer().getResource();
        
        // make sure we unreserve one with at least the same amount of
        // resources, otherwise could affect capacity limits
        if (Resources.lessThanOrEqual(rc, clusterResource,
            resourceNeedUnreserve, containerResource)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("unreserving node with reservation size: "
                + containerResource
                + " in order to allocate container with size: " + resourceNeedUnreserve);
          }
          return nodeId;
        }
      }
    }
    return null;
  }
  
  public synchronized void setHeadroomProvider(
    CapacityHeadroomProvider headroomProvider) {
    this.headroomProvider = headroomProvider;
  }

  public synchronized CapacityHeadroomProvider getHeadroomProvider() {
    return headroomProvider;
  }
  
  @Override
  public synchronized Resource getHeadroom() {
    if (headroomProvider != null) {
      return headroomProvider.getHeadroom();
    }
    return super.getHeadroom();
  }
  
  @Override
  public synchronized void transferStateFromPreviousAttempt(
      SchedulerApplicationAttempt appAttempt) {
    super.transferStateFromPreviousAttempt(appAttempt);
    this.headroomProvider = 
      ((FlowSchedulerApp) appAttempt).getHeadroomProvider();
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

  public double getMapFlowDemand() {
    FlowRate flowRate = flowSchedulerManager.lookupFlowRate(this, Type.Map);
    return flowRate.flowIn + flowRate.flowOut;
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
  
  public String getApplicationName() {
    return rmContext.getRMApps().get(getApplicationId()).getName();
  }

  // Key method to integrate RM protocol to my own implementation
  public synchronized void updateResourceRequests(List<ResourceRequest> requests) {
    for (ResourceRequest request : requests) {
      LOG.fatal("@@ request -> " + request.getRequestDetail());
    }
    super.updateResourceRequests(requests);
    LOG.fatal("[Ask] update request with size=" + requests.size());
    Random rand = new Random();
    boolean containRequestDetail = false;
    for (ResourceRequest request : requests) {
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
        Resource capability = requests.get(0).getCapability();
        addTask("AppMaster_" + this.getApplicationId(), "AppMaster_" + this.getApplicationAttemptId(), Type.AppMaster, priority, capability, null);
      }
    }
  }
  
  //my enhancement
  FlowSchedulerTask appMaster;
  ContainerId appMasterContainer;
  Map<FlowSchedulerTask, ContainerId> mapTasks = new HashMap<FlowSchedulerTask, ContainerId>();
  Map<FlowSchedulerTask, ContainerId> reduceTasks = new HashMap<FlowSchedulerTask, ContainerId>();
  List<FlowSchedulerTask> launchedTasks = new LinkedList<FlowSchedulerTask>();
  // taskAttemptId -> task
  Map<String, FlowSchedulerTask> createdTaskMap = new HashMap<String, FlowSchedulerTask>();
  FlowSchedulerManager flowSchedulerManager;
  
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

}
