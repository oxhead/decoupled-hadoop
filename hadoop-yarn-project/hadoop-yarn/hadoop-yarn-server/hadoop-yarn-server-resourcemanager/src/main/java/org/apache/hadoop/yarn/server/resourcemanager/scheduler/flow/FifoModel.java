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

public class FifoModel implements AssignmentModel {

	private static final Log LOG = LogFactory.getLog(FifoModel.class);

	List<FlowNetworkNode> nodes;
	List<FlowNetworkTask> tasks;
	List<Assignment> assignments = new LinkedList<Assignment>();

	public FifoModel(List<FlowNetworkNode> nodes, List<FlowNetworkTask> tasks) {
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
			LOG.fatal("# decide task: " + task);
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

		return new ArrayList<NetworkFlowSolver.Solution>();
	}

	@Override
	public List<Assignment> constructAssignments(List<Solution> solutions) {
		return assignments;
	}
}
