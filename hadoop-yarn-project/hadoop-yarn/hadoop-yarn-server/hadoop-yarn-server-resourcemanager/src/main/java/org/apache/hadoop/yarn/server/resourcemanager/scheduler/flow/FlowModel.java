package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FlowModel implements AssignmentModel {

	private static final Log LOG = LogFactory.getLog(FlowModel.class);

	List<FlowSchedulerNode> nodes;
	List<FlowSchedulerTask> tasks;
	List<FlowSchedulerStorage> storageNodes;

	Network net;

	CS2Solver solver;
	FlowSchedulerConfiguration conf;

	List<Solution> solutions;
	Map<Node, Object> nodeMapping = new HashMap<Node, Object>();

	int valueScale = 100;

	public FlowModel(List<FlowSchedulerNode> nodes, List<FlowSchedulerStorage> storageNodes, List<FlowSchedulerTask> tasks, FlowSchedulerConfiguration conf) {
		this.nodes = nodes;
		this.storageNodes = storageNodes;
		this.tasks = tasks;
		this.conf = conf;
		solver = new CS2Solver(conf.getSolverPath());
	}

	private int getTotalAvailableSlots() {
		int count = 0;
		for (FlowSchedulerNode node : nodes) {
			count += node.getAvailableSlots();
		}
		return count;
	}

	private int scaleValue(double value) {
		return (int) (value * valueScale);
	}

	@Override
	public void construct() {
		LOG.fatal("[Model] create min cost flow network");
		int balance = 0;
		for (FlowSchedulerTask task : tasks) {
			if (task.isMap() && task.getStorage() != null) {
				balance += 2;
			} else {
				balance += 1;
			}
		}
		net = new Network("source", "sink", balance);
		Node source = net.getSource();
		Node sink = net.getSink();
		Node unscheduledNode = net.addNode("unshceduled");

		// create task nodes
		Map<FlowSchedulerTask, Node> taskMap = new HashMap<FlowSchedulerTask, Node>();
		for (FlowSchedulerTask schedulerTask : tasks) {
			Node node = net.addNode(schedulerTask.getAttemptId());
			taskMap.put(schedulerTask, node);
			nodeMapping.put(node, schedulerTask);
		}

		// create computing nodes
		Map<FlowSchedulerNode, Node> nodeMap = new HashMap<FlowSchedulerNode, Node>();
		for (FlowSchedulerNode schedulerNode : nodes) {
			Node node = net.addNode("computing_" + schedulerNode.getHostName());
			nodeMap.put(schedulerNode, node);
			nodeMapping.put(node, schedulerNode);
		}

		// create storage nodes
		Map<FlowSchedulerStorage, Node> storageMap = new HashMap<FlowSchedulerStorage, Node>();
		for (FlowSchedulerStorage storageNode : storageNodes) {
			Node node = net.addNode("storage_" + storageNode.getHost());
			storageMap.put(storageNode, node);
		}

		// Create source->task
		for (FlowSchedulerTask task : tasks) {
			Node taskNode = taskMap.get(task);
			// one for computing, the other for storage
			if (task.isMap() && task.getStorage() != null) {
				net.addArc(source, taskNode, 2, 2, 0);
			} else {
				net.addArc(source, taskNode, 1, 1, 0);
			}

		}

		// task to other nodes
		for (FlowSchedulerTask task : tasks) {
			Node taskNode = taskMap.get(task);
			// assign to computing node
			for (FlowSchedulerNode schedulerNode : nodes) {
				Node node = nodeMap.get(schedulerNode);
				double cost = calculateTaskCost(task, schedulerNode);
				net.addArc(taskNode, node, 0, 1, scaleValue(cost));
			}

			// assign to storage node
			if (task.isMap() && task.getStorage() != null) {
				Node storageNode = storageMap.get(task.getStorage());
				double cost = calculateStorageCost(task, task.getStorage());
				net.addArc(taskNode, storageNode, 0, 1, scaleValue(cost));
			}

			// assign to unscheduled node
			double cost = calculateUnscheduledCost(task);
			net.addArc(taskNode, unscheduledNode, 0, 1, scaleValue(cost));
		}

		// computing to sink
		for (FlowSchedulerNode schedulerNode : nodes) {
			Node node = nodeMap.get(schedulerNode);
			net.addArc(node, sink, 0, schedulerNode.getAvailableSlots(), 0);
		}

		// storage to sink
		for (FlowSchedulerStorage storageNode : storageNodes) {
			Node node = storageMap.get(storageNode);
			net.addArc(node, sink, 0, tasks.size(), 0);
		}

		// Create unscheduled->sink
		net.addArc(unscheduledNode, sink, 0, tasks.size(), 0);

	}

	private double calculateEffectiveCost(double loading, double capacity) {
		if (loading > capacity) {
			return 1 + Math.pow(loading / capacity, 1.5);
		} else {
			return 0;
		}
	}

	private double calculateNodeLoading(FlowSchedulerNode node) {
		double totalDemand = 0;
		for (FlowSchedulerTask scheduelrTask : node.getRunningTasks()) {
			if (scheduelrTask.isMap()) {
				totalDemand += scheduelrTask.getFlowDemand();
			} else if (scheduelrTask.isReduce()) {
				totalDemand += calculateReduceDemand(scheduelrTask);
			}
		}
		return totalDemand;
	}

	private double calculateStorageLoading(FlowSchedulerStorage node) {
		double totalDemand = 0;
		for (FlowSchedulerTask scheduelrTask : node.getServingTasks()) {
			if (scheduelrTask.isMap()) {
				totalDemand += scheduelrTask.getFlowDemand();
			}
		}
		return totalDemand;
	}

	private double calculateStorageCost(FlowSchedulerTask task, FlowSchedulerStorage flowSchedulerStorage) {
		double totalDemand = calculateStorageLoading(flowSchedulerStorage) + task.getFlowDemand();
		return totalDemand;
	}

	private double calculateTaskCost(FlowSchedulerTask task, FlowSchedulerNode schedulerNode) {
		if (task.isAppMaster()) {
			return calculateAppMasterCost(task, schedulerNode);
		} else if (task.isMap()) {
			return calculateMapCost(task, schedulerNode);
		} else if (task.isReduce()) {
			return calculateReduceCost(task, schedulerNode);
		}
		return 0;
	}

	private double calculateAppMasterCost(FlowSchedulerTask task, FlowSchedulerNode schedulerNode) {
		return 0.0;
	}

	private double calculateMapCost(FlowSchedulerTask task, FlowSchedulerNode schedulerNode) {
		double totalDemand = calculateNodeLoading(schedulerNode) + task.getFlowDemand();
		return calculateEffectiveCost(totalDemand, schedulerNode.getCapability());
	}

	private double calculateReduceCost(FlowSchedulerTask task, FlowSchedulerNode schedulerNode) {
		double totalDemand = calculateNodeLoading(schedulerNode) + calculateReduceDemand(task);
		return calculateEffectiveCost(totalDemand, schedulerNode.getCapability());
	}

	private double calculateUnscheduledCost(FlowSchedulerTask task) {
		if (task.isAppMaster()) {
			return calculateUnscheduledAppMasterCost(task);
		} else if (task.isMap()) {
			return calculateUnscheduledMapCost(task);
		} else if (task.isReduce()) {
			return calculateUnscheduledReduceCost(task);
		}
		return 0;
	}

	// should calculate the flow demand of all map and reduce tasks?
	private double calculateUnscheduledAppMasterCost(FlowSchedulerTask task) {
		return 10000;
	}

	private double calculateUnscheduledMapCost(FlowSchedulerTask task) {
		if (task.getApp().getNumOfReduceTasks() > 0) {
			int ratio = task.getApp().getNumOfMapTasks() / task.getApp().getNumOfReduceTasks();
			int launchedMapTasks = task.getApp().getNumOfLaunchedMapTasks();
			int launchedReduceTasks = task.getApp().getNumOfLaunchedReduceTasks();
			int requiredMapTasks = launchedReduceTasks * ratio - launchedMapTasks;
			double cost = requiredMapTasks * task.getFlowDemand();
			return cost;
		} else {
			return task.getFlowDemand();
		}
	}

	private double calculateUnscheduledReduceCost(FlowSchedulerTask task) {
		int launchedMapTasks = task.getApp().getNumOfLaunchedMapTasks();
		int reduceTasks = task.getApp().getNumOfReduceTasks();
		double cost = launchedMapTasks / (double) reduceTasks * task.getApp().getMapFlowDemand();
		return cost;
	}

	private double calculateReduceDemand(FlowSchedulerTask task) {
		int numOfMapTasks = task.getApp().getNumOfMapTasks();
		int numOfReduceTasks = task.getApp().getNumOfReduceTasks();
		double reduceDemand = numOfMapTasks * task.getFlowDemand() / numOfReduceTasks;
		return reduceDemand;
	}

	@Override
	public void solve() {
		LOG.fatal("[Model] solve");
		solutions = solver.solve(net);
	}

	@Override
	public List<Assignment> constructAssignments() {
		LOG.fatal("[Model] construct assignments");
		List<Assignment> assignments = new LinkedList<Assignment>();
		for (Solution solution : solutions) {
			Node head = solution.head;
			Node tail = solution.tail;
			Object headObject = nodeMapping.get(head);
			Object tailObject = nodeMapping.get(tail);
			int flow = solution.flow;
			if (headObject instanceof FlowSchedulerTask && tailObject instanceof FlowSchedulerNode && flow > 0) {
				FlowSchedulerTask task = (FlowSchedulerTask) headObject;
				FlowSchedulerNode node = (FlowSchedulerNode) tailObject;
				Assignment assignment = new Assignment(task, node);
				assignments.add(assignment);
			}

		}
		return assignments;
	}
}

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
	int balance;
	String name;

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