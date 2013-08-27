package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkTask;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.NetworkFlowSolver.Solution;

public class BalancingModel implements AssignmentModel {

	private static final Log LOG = LogFactory.getLog(BalancingModel.class);

	List<FlowNetworkNode> nodes;
	List<FlowNetworkTask> tasks;
	List<Assignment> assignments = new LinkedList<Assignment>();

	public BalancingModel(List<FlowNetworkNode> nodes, List<FlowNetworkTask> tasks) {
		this.nodes = nodes;
		this.tasks = tasks;
	}

	public void construct() {
	}

	// TODO: here might be wrong, should assign from larger slot?
	public List<Solution> solve() {

		assignments.clear();

		Map<FlowNetworkNode, Integer> slotMap = new HashMap<FlowNetworkNode, Integer>();
		for (FlowNetworkNode node : nodes) {
			slotMap.put(node, node.getAvailableSlots());
		}

		Set<FlowNetworkTask> orderedTasks = new TreeSet<FlowNetworkTask>(new Comparator<FlowNetworkTask>() {

			@Override
			public int compare(FlowNetworkTask arg0, FlowNetworkTask arg1) {
				return arg0.flowRate.flowIn <= arg1.flowRate.flowIn ? -1 : 1;
			}
		});

		orderedTasks.addAll(tasks);
		int index = 0;
		for (FlowNetworkTask task : orderedTasks) {
			FlowNetworkNode node = nodes.get(index++ % nodes.size());
			int availableSlots = slotMap.get(node);
			if (availableSlots >= task.slot) {
				Assignment assignment = new Assignment(task, node);
				assignments.add(assignment);
				slotMap.put(node, availableSlots - task.slot);
			}
		}

		return new ArrayList<NetworkFlowSolver.Solution>();
	}

	@Override
	public List<Assignment> constructAssignments(List<Solution> solutions) {
		return assignments;
	}
}
