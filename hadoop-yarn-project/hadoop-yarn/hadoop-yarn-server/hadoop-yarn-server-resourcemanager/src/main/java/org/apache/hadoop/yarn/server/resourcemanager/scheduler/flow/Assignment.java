package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

public class Assignment {

	FlowSchedulerTask task;
	FlowSchedulerNode node;

	public Assignment(FlowSchedulerTask task, FlowSchedulerNode node) {
		this.task = task;
		this.node = node;
	}

	public FlowSchedulerTask getTask() {
		return task;
	}

	public FlowSchedulerNode getNode() {
		return node;
	}

}
