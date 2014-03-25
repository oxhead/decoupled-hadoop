package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkStorage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkTask;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.NetworkFlowSolver.Solution;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.Task.Type;

public class MinCostFlowModel implements AssignmentModel {

	class Network {
		Node source;
		Node sink;
		Map<Node, Integer> nodeIndexMap = new HashMap<Node, Integer>();
		List<Node> nodeList = new ArrayList<Node>();
		List<Arc> arcList = new LinkedList<Arc>();
		int balance;

		public Network(String source, String sink, int balance) {
			this.balance = balance;
			setSource(source, balance);
			setSink(sink, -balance);
		}

		Node setSource(String name, int balance) {
			Node source = new Node(name, balance);
			this.source = source;
			nodeIndexMap.put(source, 1);
			nodeList.add(source);
			return source;
		}

		Node setSink(String name, int balance) {
			Node sink = new Node(name, balance);
			this.sink = sink;
			nodeIndexMap.put(sink, 2);
			nodeList.add(sink);
			return sink;
		}

		int getBalance() {
			return balance;
		}

		Node getSource() {
			return this.source;
		}

		Node getSink() {
			return this.sink;
		}

		Node addNode(String name) {
			Node node = new Node(name, 0);
			nodeIndexMap.put(node, nodeIndexMap.size() + 1);
			nodeList.add(node);
			return node;
		}

		Arc addArc(Node head, Node tail, int minCap, int maxCap, int cost) {
			Arc arc = new Arc(head, tail, minCap, maxCap, cost);
			arcList.add(arc);
			return arc;
		}

		int getNodeSize() {
			return nodeIndexMap.size();
		}

		int getArcSize() {
			return arcList.size();
		}

		int getNodeIndex(Node node) {
			return nodeIndexMap.get(node);
		}

		Node getNodeByIndex(int index) {
			Node node = nodeList.get(index - 1);
			return node;

		}

	}

	class Node {
		String name;
		int balance;

		public Node(String name, int balance) {
			this.name = name;
			this.balance = balance;
		}

	}

	class Arc {
		Node head;
		Node tail;
		int minCap;
		int maxCap;
		int cost;

		public Arc(Node head, Node tail, int minCap, int maxCap, int cost) {
			this.head = head;
			this.tail = tail;
			this.minCap = minCap;
			this.maxCap = maxCap;
			this.cost = cost;
		}

		@Override
		public String toString() {
			return "FArc: " + head.name + "->" + tail.name + " => min=" + minCap + ", max=" + maxCap + ", cost=" + cost;
		}

	}

	// Information of cluster status to avoid unnecessary calculation
	class ClusterStatus {
		int maxAllowedSlots;
		double minLoading;
		int requiredSlots;
		int requiredTasks;

		List<FlowNetworkTask> tasks = new LinkedList<FlowNetworkMonitor.FlowNetworkTask>();
		List<FlowNetworkNode> nodes = new LinkedList<FlowNetworkMonitor.FlowNetworkNode>();
		List<FlowNetworkStorage> storages = new LinkedList<FlowNetworkMonitor.FlowNetworkStorage>();

		@Override
		public String toString() {
			return "requiredTasks=" + requiredTasks + ", requiredSlots=" + requiredSlots + ", maxAllowedSlot=" + maxAllowedSlots + ", minLoading=" + minLoading;
		}
	}

	private static final Log LOG = LogFactory.getLog(MinCostFlowModel.class);

	List<FlowNetworkNode> nodes;
	List<FlowNetworkTask> tasks;
	List<FlowNetworkStorage> storageNodes;
	Map<Node, Object> nodeMap = new HashMap<Node, Object>();
	Network net;
	NetworkFlowSolver solver;

	public MinCostFlowModel(List<FlowNetworkNode> nodes, List<FlowNetworkStorage> storageNodes, List<FlowNetworkTask> tasks, NetworkFlowSolver solver) {
		this.nodes = nodes;
		this.storageNodes = storageNodes;
		this.tasks = tasks;
		this.solver = solver;
	}

	private ClusterStatus constructClusterStatus(List<FlowNetworkNode> nodes, List<FlowNetworkTask> tasks) {
		ClusterStatus clusterStatus = new ClusterStatus();

		int requiredTasks = tasks.size();
		clusterStatus.requiredTasks = requiredTasks;

		int requiredSlots = 0;
		for (FlowNetworkTask task : tasks) {
			requiredSlots += task.slot;
		}
		clusterStatus.requiredSlots = requiredSlots;

		int availableSlots = 0;
		for (FlowNetworkNode node : nodes) {
			availableSlots += node.getAvailableSlots();
		}
		clusterStatus.maxAllowedSlots = availableSlots;

		double minLoading = Double.MAX_VALUE;
		for (FlowNetworkNode node : nodes) {
			if (node.getEffectiveLoad() < minLoading) {
				minLoading = node.getEffectiveLoad();
			}
		}
		clusterStatus.minLoading = minLoading;

		return clusterStatus;
	}

	private Map<FlowNetworkTask, FlowNetworkNode> assignAppMaster(List<FlowNetworkTask> tasks, List<FlowNetworkNode> nodes) {
		Map<FlowNetworkTask, FlowNetworkNode> appMasterAssignment = new HashMap<FlowNetworkMonitor.FlowNetworkTask, FlowNetworkMonitor.FlowNetworkNode>();
		Map<FlowNetworkNode, Integer> map = new HashMap<FlowNetworkMonitor.FlowNetworkNode, Integer>();
		for (FlowNetworkNode node : nodes) {
			int numOfAppMaster = 0;
			for (FlowNetworkTask task : node.launchedTasks) {
				if (task.type.equals(Type.AppMaster)) {
					numOfAppMaster++;
				}
			}
			map.put(node, numOfAppMaster);
		}
		for (FlowNetworkTask task : tasks) {
			if (task.type.equals(Type.AppMaster)) {
				FlowNetworkNode node = pickNode(map);
				appMasterAssignment.put(task, node);
				map.put(node, map.get(node) + 1);
			}
		}
		return appMasterAssignment;
	}

	private FlowNetworkNode pickNode(Map<FlowNetworkNode, Integer> map) {
		int min = Integer.MAX_VALUE;
		FlowNetworkNode minNode = null;
		for (Map.Entry<FlowNetworkNode, Integer> entry : map.entrySet()) {
			if (entry.getValue() < min) {
				min = entry.getValue();
				minNode = entry.getKey();
			}
		}
		return minNode;
	}

	@Override
	public void construct() {
		LOG.fatal("[Model] create min cost flow network");
		int cost_scale = 100;

		ClusterStatus clusterStatus = constructClusterStatus(nodes, tasks);
		Map<FlowNetworkTask, FlowNetworkNode> appMasterAssignment = assignAppMaster(tasks, nodes);

		LOG.fatal("[Model] cluster status: " + clusterStatus);

		net = new Network("source", "sink", clusterStatus.requiredTasks);
		Node source = net.getSource();
		Node sink = net.getSink();
		Node unscheduledNode = net.addNode("unshceduled");

		// Create lookup table for later reference
		Map<FlowNetworkNode, Node> my_node_map = new HashMap<FlowNetworkNode, Node>();
		for (FlowNetworkNode node : nodes) {
			Node my_node = net.addNode(node.toString());
			my_node_map.put(node, my_node);
			nodeMap.put(my_node, node);
		}
		Map<FlowNetworkTask, Node> my_task_map = new HashMap<FlowNetworkTask, Node>();
		for (FlowNetworkTask task : tasks) {
			Node my_node = net.addNode(task.toString());
			my_task_map.put(task, my_node);
			nodeMap.put(my_node, task);
		}

		// Create source->task, no cost
		for (FlowNetworkTask task : tasks) {

			Node taskNode = my_task_map.get(task);
			// Create source to task node
			// TODO: AppMaster should take 3 slots. Currently consider only task
			// unit.
			Arc my_arc = net.addArc(source, taskNode, 1, 1, 0);
			// LOG.fatal("* [Arc]" + my_arc);
		}

		// Create task->computing, task->unscheduled
		for (Map.Entry<FlowNetworkTask, Node> task_entry : my_task_map.entrySet()) {

			FlowNetworkTask task = task_entry.getKey();
			Node taskNode = task_entry.getValue();

			// AppMaster must be assigned first
			if (task.type.equals(Type.AppMaster)) {
				FlowNetworkNode targetNode = appMasterAssignment.get(task);
				for (Map.Entry<FlowNetworkNode, Node> entry : my_node_map.entrySet()) {
					FlowNetworkNode node = entry.getKey();
					Node computingNode = entry.getValue();
					if (node.equals(targetNode)) {
						// TODO: really need to start all AppMaster?
						Arc arc = net.addArc(taskNode, computingNode, 1, 1, 0);
						LOG.fatal("+ AppMaster will be assigned to " + targetNode);
					} else {
						Arc arc = net.addArc(taskNode, computingNode, 0, 0, 0);
						// LOG.fatal("+ AppMaster: " + arc);
					}
				}
			} else {
				for (Map.Entry<FlowNetworkNode, Node> entry : my_node_map.entrySet()) {
					FlowNetworkNode node = entry.getKey();
					Node computingNode = entry.getValue();
					// TODO: partially solve load balancing problem, workaround
					// solution
					double cost = calculateTaskCost(node, task, clusterStatus);
					Arc arc = net.addArc(taskNode, computingNode, 0, 1, (int) (cost * cost_scale));
					// LOG.fatal("@ [Arc] " + arc);
				}
			}

			double cost = calculateUnscheduleCost(task, clusterStatus);
			Arc arc = net.addArc(taskNode, unscheduledNode, 0, 1, (int) (cost * cost_scale));
			// LOG.fatal("# [Arc] " + arc);
		}

		// Create computing->sink
		for (Map.Entry<FlowNetworkNode, Node> entry : my_node_map.entrySet()) {
			Node computingNode = entry.getValue();
			// TODO: should modify
			// double cost = -entry.getKey().getSlotCapability();
			// Arc arc = net.addArc(computingNode, sink, 0,
			// entry.getKey().getAvailableSlots(), (int) (cost * cost_scale));
			double cost = 1 + entry.getKey().getEffectiveLoad() / entry.getKey().capacity;
			Arc arc = net.addArc(computingNode, sink, 0, entry.getKey().getAvailableSlots(), (int) (cost * cost_scale));
			// LOG.fatal("$ [Arc] " + arc);
		}

		// Create unscheduled->sink
		// TODO: might have problem if slot size is not 512MB, e,g, AppMaster
		int numOfUnscheduledTasks = clusterStatus.requiredTasks - clusterStatus.maxAllowedSlots;
		numOfUnscheduledTasks = numOfUnscheduledTasks < 0 ? 0 : numOfUnscheduledTasks;
		// numOfUnscheduledTasks = numOfTasks;
		Arc arc = net.addArc(unscheduledNode, sink, 0, numOfUnscheduledTasks, 0);
		// LOG.fatal("+ [Arc] " + arc);
		LOG.fatal("@@ total: " + clusterStatus.requiredTasks);
		LOG.fatal("@@ available: " + clusterStatus.maxAllowedSlots);
		LOG.fatal("@@ unscheduled: " + numOfUnscheduledTasks);
	}

	private double calculateTaskCost(FlowNetworkNode node, FlowNetworkTask task, ClusterStatus clusterStatus) {
		double cost = 0;
		if (task.type.equals(Type.AppMaster)) {
			cost = calculateAppMasterCost(node, task, clusterStatus);
		} else if (task.type.equals(Type.Map)) {
			cost = calculateMapCost(node, task, clusterStatus);
			//double randomCost = new Random().nextInt(10) / 10.0;
			//cost += randomCost;
		} else if (task.type.equals(Type.Reduce)) {
			cost = calculateReduceCost(node, task, clusterStatus);
			//double randomCost = new Random().nextInt(10);
			//cost += randomCost;
		}
		return cost;
	}

	private double calculateAppMasterCost(FlowNetworkNode node, FlowNetworkTask task, ClusterStatus clusterStatus) {
		return 0;
		// double freeCapability = node.capacity - node.getEffectiveLoad();
		// return freeCapability;
	}

	// TODO: only flowin cost?
	private double calculateUnscheduleCost(FlowNetworkTask task, ClusterStatus clusterStatus) {
		if (task.type.equals(Type.Map)) {
			return task.flowRate.flowIn * 2;
		} else if (task.type.equals(Type.Reduce)) {
			return (task.app.getNumOfMapTasks() / (double) task.app.getNumOfReduceTasks()) * (task.flowRate.flowIn + task.flowRate.flowOut) * 10;
		} else if (task.type.equals(Type.AppMaster)) {
			// force the AppMaster to be scheduled
			return 30;
		}
		return 0;
	}

	private double calculateMapCost(FlowNetworkNode node, FlowNetworkTask task, ClusterStatus clusterStatus) {
		double loading = (task.flowRate.flowIn + node.getEffectiveLoad()) / node.capacity;
		double cost = Math.exp(loading);
		// double cost_processing = calculateMapFlowProcessingPenaltyCost(task,
		// node, clusterStatus);
		// double cost_flowin = calculateMapFlowInQualityCost(task,
		// task.storage);
		// double cost = cost_processing + cost_flowin;
		// LOG.fatal("[MC] " + task + "->" + node + "\n\tload=" +
		// node.getEffectiveLoad() + ", slot_capacity=" +
		// node.getSlotCapability() + ", " + "require=" + task.flowRate.flowIn +
		// " => cost=" + cost_processing);
		return cost;
	}

	private double calculateReduceCost(FlowNetworkNode node, FlowNetworkTask task, ClusterStatus clusterStatus) {
		double flowDemand = (task.app.getNumOfMapTasks() / task.app.getNumOfReduceTasks()) * (task.flowRate.flowIn + task.flowRate.flowOut);
		double loading = (flowDemand + node.getEffectiveLoad()) / node.capacity;
		double cost = Math.exp(loading);
		// double cost_processing =
		// calculateReduceFlowProcessingQualityCost(task, node, clusterStatus);
		// double cost_flowin = calculateReduceFlowInQualityCost(task, node,
		// clusterStatus);
		// double cost_flowout = calculateReduceFlowOutQualityCost(task,
		// clusterStatus);
		// double cost = cost_processing + cost_flowin + cost_flowout;
		// LOG.fatal("[RC] " + task + "->" + node + "\n\tload=" +
		// node.getEffectiveLoad() + ", slot_capacity=" +
		// node.getSlotCapability() + ", " + "require=" + task.flowRate.flowIn +
		// " => cost="+ cost_processing);
		return cost;
	}

	private double calculateMapFlowProcessingPenaltyCost(FlowNetworkTask task, FlowNetworkNode node, ClusterStatus status) {
		double slot_capacity = node.getSlotCapability();
		double cost = 0;
		if (task.flowRate.flowIn > slot_capacity) {
			cost = task.flowRate.flowIn - slot_capacity;
		}
		// cost = Math.abs(task.flowRate.flowIn - slot_capacity);
		return cost;
	}

	// TODO: better model
	private double calculateMapFlowInQualityCost(FlowNetworkTask task, FlowNetworkStorage storage) {
		// need weight?
		if (storage != null) {
			return (1 + storage.load / storage.capacity) * task.flowRate.flowIn;
		} else {
			return 0;
		}
	}

	// load balancing
	private double calculateReduceFlowProcessingQualityCost(FlowNetworkTask task, FlowNetworkNode node, ClusterStatus status) {
		double reduce_total_capability = calculateEffectiveReduceCapability(task);
		double slot_capacity = node.getSlotCapability();
		double cost = 0;
		if (reduce_total_capability > slot_capacity) {
			cost = reduce_total_capability - slot_capacity;
		}
		// cost = Math.abs(reduce_total_capability - slot_capacity);
		return cost;
	}

	private double calculateEffectiveReduceCapability(FlowNetworkTask task) {
		int numOfMapTasks = task.app.getNumOfMapTasks();
		int numOfReduceTasks = task.app.getNumOfReduceTasks();
		double effective_capability = numOfMapTasks / numOfReduceTasks * task.flowRate.flowIn;
		return effective_capability;
	}

	// TODO: better model
	private double calculateReduceFlowInQualityCost(FlowNetworkTask task, FlowNetworkNode node, ClusterStatus clusterStatus) {
		int numberOfReduceTasks = task.app.getNumOfReduceTasks();
		// better way? not just capacity, should have flowIn and flowOut in task
		double reduceFlowInRateRatio = 1.0 / numberOfReduceTasks;
		double cost = 0;
		for (FlowNetworkTask mapTask : task.app.getMapTasks()) {
			FlowNetworkNode mapNode = mapTask.node;
			// TODO: seems reduce tasks start earlier, cost model might not be
			// accurate
			if (mapNode != null) {
				double effictiveMapFlowOut = reduceFlowInRateRatio * task.flowRate.flowIn;
				// TODO: should take care about rack -> calculate rack
				if (node.equals(mapNode)) {
					// LOG.fatal("<> same node: " + node);

				} else if (inSameRack(node, mapNode)) {
					double mapFlowOutCost = (1 + mapNode.getEffectiveLoad() / mapNode.capacity) * effictiveMapFlowOut / 2;
					cost += mapFlowOutCost;
					// LOG.fatal("<> same rack: " + node + " -> " + mapNode);
				} else {
					double mapFlowOutCost = (1 + mapNode.getEffectiveLoad() / mapNode.capacity) * effictiveMapFlowOut * 5;
					cost += mapFlowOutCost;
					// LOG.fatal("<> off rack: " + node + " -> " + mapNode);
				}

			} else {
				LOG.fatal("@_@ seems task hasn't been launched: " + task);
			}

		}
		return cost;
	}

	private boolean inSameRack(FlowNetworkNode node1, FlowNetworkNode node2) {
		// LOG.fatal("[Compare] host1: " + node1.node.getHostName() +
		// ", host2: " + node2.node.getHostName());
		String host1 = node1.node.getHostName();
		String host2 = node2.node.getHostName();
		CharSequence suffix1 = host1.subSequence(host1.length() - 5, host1.length());
		CharSequence suffix2 = host2.subSequence(host2.length() - 5, host2.length());
		return suffix1.equals(suffix2);
	}

	// TODO: better model
	private double calculateReduceFlowOutQualityCost(FlowNetworkTask task, ClusterStatus status) {
		int numOfMapTasks = task.app.getNumOfMapTasks();
		int numOfReduceTasks = task.app.getNumOfReduceTasks();
		// TODO: this should be R_out for reduce, not capacity
		double effective_capability = numOfMapTasks / numOfReduceTasks * task.flowRate.flowIn;
		double cost = 0;
		for (FlowNetworkStorage storage : status.storages) {
			double cost_storage = (1 + storage.capacity / storage.load) * effective_capability;
			cost += cost_storage;
		}
		return cost;
	}

	@Override
	public List<Solution> solve() {
		LOG.fatal("[Model] solve");
		List<Solution> solutions = solver.solve(net);
		return solutions;
	}

	@Override
	public List<Assignment> constructAssignments(List<Solution> solutions) {
		LOG.fatal("[Model] construct assignments");
		List<Assignment> assignments = new LinkedList<Assignment>();
		for (Solution solution : solutions) {
			Node head = solution.head;
			Node tail = solution.tail;
			Object headObject = nodeMap.get(head);
			Object tailObject = nodeMap.get(tail);
			int flow = solution.flow;
			if (headObject instanceof FlowNetworkTask && tailObject instanceof FlowNetworkNode && flow > 0) {
				FlowNetworkTask task = (FlowNetworkTask) headObject;
				FlowNetworkNode node = (FlowNetworkNode) tailObject;
				Assignment assignment = new Assignment(task, node);
				assignments.add(assignment);
			}

		}
		return assignments;
	}
}
