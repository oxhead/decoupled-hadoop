package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.LinkedList;
import java.util.List;

class FlowSchedulerStorage {
  private String host;
  private int capacity;
  private int load;

  private List<FlowSchedulerTask> servingTasks = new LinkedList<FlowSchedulerTask>();

  public FlowSchedulerStorage(String host, int capacity) {
    this.host = host;
    this.capacity = capacity;
    this.load = 0;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getCapacity() {
    return capacity;
  }

  public void setCapacity(int capacity) {
    this.capacity = capacity;
  }

  void launchTask(FlowSchedulerTask task) {
    servingTasks.add(task);
    this.load += task.getFlowDemand();
  }

  void completeTask(FlowSchedulerTask task) {
    servingTasks.remove(task);
    this.load -= task.getFlowDemand();
  }

  public int getLoad() {
    return load;
  }

  public List<FlowSchedulerTask> getServingTasks() {
    return servingTasks;
  }

  @Override
  public String toString() {
    return "FlowNetworkStorage [host=" + host + ", capacity=" + capacity + "]";
  }
}