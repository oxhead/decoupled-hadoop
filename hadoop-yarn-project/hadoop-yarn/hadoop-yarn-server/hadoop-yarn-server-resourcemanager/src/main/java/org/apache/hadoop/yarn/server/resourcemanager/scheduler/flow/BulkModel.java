package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkTask;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.NetworkFlowSolver.Solution;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.Task.Type;

public class BulkModel implements AssignmentModel {

	private static final Log LOG = LogFactory.getLog(BulkModel.class);

	List<FlowNetworkNode> nodes;
	List<FlowNetworkTask> tasks;
	List<Assignment> assignments = new LinkedList<Assignment>();

	public BulkModel(List<FlowNetworkNode> nodes, List<FlowNetworkTask> tasks) {
		this.nodes = nodes;
		this.tasks = tasks;
	}

	public void construct() {
	}

	public List<Solution> solve() {

		assignments.clear();

		Map<FlowNetworkNode, Integer> slotMap = new HashMap<FlowNetworkNode, Integer>();
		for (FlowNetworkNode node : nodes) {
			slotMap.put(node, node.getAvailableSlots());
		}

		LOG.fatal("# nodes: " + nodes.size());
		LOG.fatal("# tasks: " + tasks.size());

		for (FlowNetworkTask task : tasks) {
			if (task.type.equals(Type.AppMaster)) {
				for (int i = nodes.size() - 1; i >= 0; i--) {
					FlowNetworkNode node = nodes.get(i);
					int availableSlots = slotMap.get(node);
					if (availableSlots >= task.slot) {
						Assignment assignment = new Assignment(task, node);
						assignments.add(assignment);
						slotMap.put(node, availableSlots - task.slot);
						break;
					}
				}
			}
		}

		for (FlowNetworkTask task : tasks) {
			if (!task.type.equals(Type.AppMaster)) {
				for (FlowNetworkNode node : nodes) {
					LOG.fatal("# decide node: " + node);
					int availableSlots = slotMap.get(node);
					if (availableSlots >= task.slot) {
						Assignment assignment = new Assignment(task, node);
						assignments.add(assignment);
						slotMap.put(node, availableSlots - task.slot);
						break;
					}
				}
			}
		}

		return new ArrayList<NetworkFlowSolver.Solution>();
	}

	@Override
	public List<Assignment> constructAssignments(List<Solution> solutions) {
		return assignments;
	}
}
