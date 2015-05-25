package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowSchedulerTask.Type;

public class FlowSchedulerManager {

  private static final Log LOG = LogFactory.getLog(FlowSchedulerManager.class);

  public static final int PRIORITY_APPMASTER = 0;
  public static final int PRIORITY_MAP = 20;
  public static final int PRIORITY_REDUCE = 10;

  RMContext rmContext;
  FlowSchedulerConfiguration conf;

  // attemptid->app
  Map<ApplicationAttemptId, FlowSchedulerApp> appMap = new HashMap<ApplicationAttemptId, FlowSchedulerApp>();
  // host->node
  Map<NodeId, FlowSchedulerNode> nodeMap = new HashMap<NodeId, FlowSchedulerNode>();
  // host->storage
  Map<String, FlowSchedulerStorage> storageMap = new HashMap<String, FlowSchedulerStorage>();

  Map<ContainerId, FlowSchedulerTask> containerTaskMap = new HashMap<ContainerId, FlowSchedulerTask>();

  public FlowSchedulerManager(RMContext rmContext, FlowSchedulerConfiguration conf) {
    this.rmContext = rmContext;
    this.conf = conf;

  }

  public int getNumOfApps() {
    return appMap.size();
  }

  public void addApp(FlowSchedulerApp app) {
    LOG.fatal("Add app: " + app.getApplicationAttemptId());
    appMap.put(app.getApplicationAttemptId(), app);
  }

  public void removeApp(ApplicationAttemptId appId) {
    LOG.fatal("Remove app: " + appId);
    appMap.remove(appId);
  }

  public FlowSchedulerApp lookupApp(ApplicationAttemptId appId) {
    return appMap.get(appId);
  }

  public List<FlowSchedulerApp> getApps() {
    return new LinkedList<FlowSchedulerApp>(appMap.values());
  }

  public List<FlowSchedulerApp> getSchedulableApps() {
    List<FlowSchedulerApp> schedulableApps = new ArrayList<FlowSchedulerApp>();
    for (FlowSchedulerApp app : getApps()) {
      if (app.hasUnlaunchedTasks()) {
        schedulableApps.add(app);
      }
    }
    return schedulableApps;
  }

  public void addNode(FlowSchedulerNode node) {
    LOG.fatal("Add node: " + node.getNodeName());
    nodeMap.put(node.getNodeID(), node);
  }

  public void removeNode(NodeId nodeId) {
    LOG.fatal("Remove node: " + nodeId.getHost());
    nodeMap.remove(nodeId);
  }

  public FlowSchedulerNode lookupNode(NodeId nodeId) {
    return nodeMap.get(nodeId);
  }

  public int getNumOfNodes() {
    return nodeMap.size();
  }

  public List<FlowSchedulerNode> getNodes() {
    return new LinkedList<FlowSchedulerNode>(nodeMap.values());
  }

  public List<FlowSchedulerNode> getAvailableNodes() {
    List<FlowSchedulerNode> availableNodes = new LinkedList<FlowSchedulerNode>();
    for (FlowSchedulerNode node : getNodes()) {
      if (node.isAvailable()) {
        availableNodes.add(node);
      }
    }
    return availableNodes;
  }

  public List<FlowSchedulerStorage> getStorages() {
    return new LinkedList<FlowSchedulerStorage>(storageMap.values());
  }

  public FlowSchedulerStorage lookupStorage(String host) {
    if (host == null) {
      return null;
    }
    if (!storageMap.containsKey(host)) {
      FlowSchedulerStorage storage = new FlowSchedulerStorage(host, 80);
      storageMap.put(host, storage);
    }
    return storageMap.get(host);
  }

  public String getRandomDataHost(List<String> dataHosts) {
    if (dataHosts == null || dataHosts.size() == 0) {
      return null;
    }
    int randomIndex = new Random().nextInt(dataHosts.size());
    return dataHosts.get(randomIndex);
  }

  public FlowRate lookupFlowRate(FlowSchedulerApp app, Type type) {
    FlowRate flowRate;
    if (type.equals(Type.AppMaster)) {
      flowRate = new FlowRate(0, 0);
    } else {
      flowRate = conf.getFlowRate(app.getApplicationName(), type);
    }
    return flowRate;
  }

  public List<FlowSchedulerTask> getUnscheduledTasks() {
    List<FlowSchedulerTask> unscheduledTasks = new LinkedList<FlowSchedulerTask>();
    for (FlowSchedulerApp app : appMap.values()) {
      if (app.hasUnlaunchedTasks()) {
        unscheduledTasks.addAll(app.getUnscheduledTasks());
      }
    }
    return unscheduledTasks;
  }

  public FlowSchedulerTask lookupTask(ContainerId containerId) {
    return containerTaskMap.get(containerId);
  }

  // after allocation before launched on node
  public synchronized void launchTask(FlowSchedulerTask task, RMContainer rmContainer) {
    FlowSchedulerApp app = task.getApp();
    FlowSchedulerNode node = lookupNode(rmContainer.getContainer().getNodeId());
    FlowSchedulerStorage storage = task.getStorage();
    app.launchTask(task, node, rmContainer);
    node.launchTask(task, rmContainer);
    if (storage != null) {
      storage.launchTask(task);
    }
    containerTaskMap.put(rmContainer.getContainerId(), task);
  }

  public synchronized void completeTask(FlowSchedulerTask task, ContainerStatus containerStatus, RMContainerEventType event) {
    FlowSchedulerApp app = task.getApp();
    FlowSchedulerNode node = task.getNode();
    FlowSchedulerStorage storage = task.getStorage();
    app.completeTask(task, containerStatus, event);
    node.completeTask(task);
    if (storage != null) {
      storage.completeTask(task);
    }
    containerTaskMap.remove(task.getRmContainer().getContainerId());
  }

  // confirm from node manager
  public void launchedContainer(ContainerId containerId, NodeId nodeId) {

    // Get the application for the finished container
    ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
    FlowSchedulerApp application = lookupApp(applicationAttemptId);
    FlowSchedulerNode node = lookupNode(nodeId);
    if (application == null) {
      LOG.info("Unknown application: " + applicationAttemptId + " launched container " + containerId + " on node: " + node);
      // Some unknown container sneaked into the system. Kill it.
      this.rmContext.getDispatcher().getEventHandler().handle(new RMNodeCleanContainerEvent(node.getNodeID(), containerId));

      return;
    }

    application.containerLaunchedOnNode(containerId, node.getNodeID());
  }

}