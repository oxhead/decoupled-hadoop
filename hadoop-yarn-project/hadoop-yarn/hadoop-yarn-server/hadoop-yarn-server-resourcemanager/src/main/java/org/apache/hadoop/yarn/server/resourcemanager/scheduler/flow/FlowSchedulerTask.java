package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

class FlowSchedulerTask {

  enum Type {
    AppMaster, Map, Reduce, UNKNOWN
  };

  public static final int PRIORITY_APPMASTER = 0;
  public static final int PRIORITY_MAP = 20;
  public static final int PRIORITY_REDUCE = 10;

  private String taskId;
  private String attemptId;
  private Type type;
  private Priority priority;
  private FlowRate flowDemand;
  private Resource resourceDemand;
  private FlowSchedulerApp app;
  private FlowSchedulerNode node;
  private FlowSchedulerStorage storage;
  private RMContainer rmContainer;

  private boolean running = false;
  private boolean completed = false;

  public FlowSchedulerTask(String taskId, String attemptId, Type type, Priority priority, FlowRate flowDemand, Resource resourceDemand, FlowSchedulerApp app, FlowSchedulerStorage storage) {
    this.taskId = taskId;
    this.attemptId = attemptId;
    this.type = type;
    this.priority = priority;
    this.flowDemand = flowDemand;
    this.resourceDemand = resourceDemand;
    this.app = app;
    this.storage = storage;
  }

  public static Type getType(String typeName) {
    if (typeName.equalsIgnoreCase("map")) {
      return Type.Map;
    } else if (typeName.equalsIgnoreCase("reduce")) {
      return Type.Reduce;
    } else if (typeName.equalsIgnoreCase("appmaster")) {
      return Type.AppMaster;
    }
    return Type.UNKNOWN;
  }

  public static Type getType(int priority) {
    switch (priority) {
    case PRIORITY_APPMASTER:
      return Type.AppMaster;
    case PRIORITY_MAP:
      return Type.Map;
    case PRIORITY_REDUCE:
      return Type.Reduce;
    default:
      break;
    }
    return Type.Map;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public Priority getPriority() {
    return priority;
  }

  public void setPriority(Priority priority) {
    this.priority = priority;
  }

  public double getFlowDemand() {
    return flowDemand.flowIn + flowDemand.flowOut;
  }

  public FlowRate getRawFlowDemand() {
    return flowDemand;
  }

  public void setFlowDemand(FlowRate flowDemand) {
    this.flowDemand = flowDemand;
  }

  public Resource getResourceDemand() {
    return resourceDemand;
  }

  public void setResourceDemand(Resource resourceDemand) {
    this.resourceDemand = resourceDemand;
  }

  public FlowSchedulerApp getApp() {
    return app;
  }

  public void setApp(FlowSchedulerApp app) {
    this.app = app;
  }

  public FlowSchedulerNode getNode() {
    return node;
  }

  public void setNode(FlowSchedulerNode node) {
    this.node = node;
  }

  public FlowSchedulerStorage getStorage() {
    return storage;
  }

  public void setStorage(FlowSchedulerStorage storage) {
    this.storage = storage;
  }

  public String getAttemptId() {
    return attemptId;
  }

  public void setAttemptId(String attemptId) {
    this.attemptId = attemptId;
  }

  public RMContainer getRmContainer() {
    return rmContainer;
  }

  public void setRmContainer(RMContainer rmContainer) {
    this.rmContainer = rmContainer;
  }

  public void launch(FlowSchedulerNode node, RMContainer rmContainer) {
    this.node = node;
    this.rmContainer = rmContainer;
    this.running = true;
  }

  public void complete() {
    this.running = false;
    this.completed = true;
  }

  public boolean isLaunched() {
    return this.rmContainer != null;
  }

  public boolean isCompleted() {
    return completed;
  }

  public boolean isUnscheduled() {
    return !isLaunched();
  }

  public boolean isMap() {
    return this.type.equals(Type.Map);
  }

  public boolean isReduce() {
    return this.type.equals(Type.Reduce);
  }

  public boolean isAppMaster() {
    return this.type.equals(Type.AppMaster);
  }

  @Override
  public String toString() {
    return "FlowSchedulerTask [attemptId=" + attemptId + ", type=" + type + ", priority=" + priority + ", flowDemand=" + flowDemand + ", resourceDemand=" + resourceDemand + ", app="
        + app.getApplicationName() + ", node=" + node + ", storage=" + storage + ", rmContainer=" + rmContainer + ", running=" + running + ", completed=" + completed + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((attemptId == null) ? 0 : attemptId.hashCode());
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
    FlowSchedulerTask other = (FlowSchedulerTask) obj;
    if (attemptId == null) {
      if (other.attemptId != null)
        return false;
    } else if (!attemptId.equals(other.attemptId))
      return false;
    return true;
  }

}