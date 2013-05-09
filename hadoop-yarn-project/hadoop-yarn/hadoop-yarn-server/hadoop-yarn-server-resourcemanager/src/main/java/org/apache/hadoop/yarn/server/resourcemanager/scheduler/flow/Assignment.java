package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkTask;

public class Assignment {

	FlowNetworkTask task;
	FlowNetworkNode node;

	public Assignment(FlowNetworkTask task, FlowNetworkNode node) {
		this.task = task;
		this.node = node;
	}

	public FlowNetworkTask getTask() {
		return task;
	}

	public FlowNetworkNode getNode() {
		return node;
	}

}
