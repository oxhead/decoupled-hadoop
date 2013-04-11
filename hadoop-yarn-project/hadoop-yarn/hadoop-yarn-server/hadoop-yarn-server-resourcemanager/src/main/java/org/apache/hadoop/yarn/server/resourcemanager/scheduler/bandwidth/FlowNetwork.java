/**
 * 
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.bandwidth;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;

import JaCoP.constraints.netflow.NetworkBuilder;
import JaCoP.constraints.netflow.NetworkFlow;
import JaCoP.constraints.netflow.simplex.Arc;
import JaCoP.constraints.netflow.simplex.Node;
import JaCoP.core.Domain;
import JaCoP.core.IntDomain;
import JaCoP.core.IntVar;
import JaCoP.core.Store;
import JaCoP.search.DepthFirstSearch;
import JaCoP.search.Indomain;
import JaCoP.search.IndomainRandom;
import JaCoP.search.InputOrderSelect;
import JaCoP.search.Search;
import JaCoP.search.SelectChoicePoint;

/**
 * @author oxhead
 * 
 */
public class FlowNetwork {

	private static final Log LOG = LogFactory.getLog(FlowNetwork.class);

	public enum Type {
		AppMaster, Map, Reduce
	};

	public static final int PRIORITY_APPMASTER = 0;
	public static final int PRIORITY_MAP = 20;
	public static final int PRIORITY_REDUCE = 10;

	// JaCoP store for constraints
	MinCostFlowNetwork mcfn = null;

	RMContext rmContext;
	BandwidthAwareSchedulerConfiguration conf;
	Map<RMContainer, ResourceRequest> containerRequestMap;

	List<FlowNetworkTask> tasks = new ArrayList<FlowNetworkTask>();
	Map<FiCaSchedulerNode, FlowNetworkNode> nodeMap = new HashMap<FiCaSchedulerNode, FlowNetworkNode>();
	Map<FiCaSchedulerApp, FlowNetworkApp> appMap = new HashMap<FiCaSchedulerApp, FlowNetworkApp>();

	int timestamp = 0;

	public FlowNetwork(RMContext rmContext, BandwidthAwareSchedulerConfiguration conf, Map<RMContainer, ResourceRequest> containerRequestMap) {
		this.rmContext = rmContext;
		this.conf = conf;
		this.containerRequestMap = containerRequestMap;
	}

	public void addNode(FiCaSchedulerNode node) {
		LOG.fatal("[Network] add node: " + node.getHostName());
		FlowNetworkNode flowNode = new FlowNetworkNode(node, 100, 0);
		nodeMap.put(node, flowNode);
	}

	public void removeNode(FiCaSchedulerNode node) {
		LOG.fatal("[Network] remove node: " + node.getHostName());
		nodeMap.remove(node);
	}

	public void updateNode(FiCaSchedulerNode node) {
		LOG.fatal("[Network] update node: " + node.getHostName());
		FlowNetworkNode flowNode = nodeMap.get(node);
		flowNode.update();
	}

	public void addApp(FiCaSchedulerApp app) {
		LOG.fatal("[Network] add app: " + app.getApplicationAttemptId());
		FlowNetworkApp flowApp = new FlowNetworkApp(app);
		appMap.put(app, flowApp);
	}

	public void removeApp(FiCaSchedulerApp app) {
		LOG.fatal("[Network] remove app: " + app.getApplicationAttemptId());
		appMap.remove(app);
	}

	public void update(int time) {
		LOG.fatal("[Network] update with time: " + time);

		this.timestamp += time;
		for (FlowNetworkNode node : nodeMap.values()) {
			node.update();
		}

		for (FlowNetworkApp app : appMap.values()) {
			app.update();
		}
	}

	public void printNodeStatus() {
		for (FlowNetworkNode node : nodeMap.values()) {
			LOG.fatal(node.getNodeStatus());
		}
	}

	public void construct() {
		LOG.fatal("[Network] construct");

		List<FlowNetworkNode> nodes = new ArrayList<FlowNetwork.FlowNetworkNode>();
		nodes.addAll(nodeMap.values());

		tasks.clear();

		int maxNumOfSchedulingTasks = conf.getMaxNumOfSchedulingTasks();
		for (FlowNetworkApp flowNetworkApp : appMap.values()) {
			List<FlowNetworkTask> appTasks = flowNetworkApp.getTasks();
			if (tasks.size() + appTasks.size() <= maxNumOfSchedulingTasks || tasks.size() + appTasks.size() < maxNumOfSchedulingTasks + 20) {
				tasks.addAll(appTasks);
			} else {
				LOG.fatal("[Network] exceeds the scheduling capability: " + "current=" + tasks.size() + ", total=" + (tasks.size() + appTasks.size()) + ", max=" + maxNumOfSchedulingTasks);
			}

		}

		constructMinCostFlowNetwork(nodes, tasks);
	}

	private void constructMinCostFlowNetwork(List<FlowNetworkNode> nodes, List<FlowNetworkTask> tasks) {
		LOG.fatal("[Network] construct min cost flow network");

		mcfn = new MinCostFlowNetwork(nodes, tasks);
		mcfn.createNetwork();
	}

	private int getExpectedCapacity(List<FlowNetworkTask> tasks) {
		int sum = 0;
		for (FlowNetworkTask task : tasks) {
			sum += task.capacity;
		}
		return (int) ((double) sum / (double) tasks.size());
	}

	private int getMinLoading(List<FlowNetworkNode> nodes) {
		int min = Integer.MAX_VALUE;
		for (FlowNetworkNode node : nodes) {
			if (node.load < min) {
				min = node.load;
			}
		}
		return min;
	}

	/**
	 * @param capacity
	 *            task capacity
	 * @param node_load
	 *            load on the load
	 * @param node_load_min
	 *            min load among all nodes
	 * @return
	 */
	private int convertCost(double capacity, double slot_capacity, double node_load, double node_load_min) {
		int cost = (int) ((node_load - node_load_min) + (capacity - slot_capacity));
		if (cost > 1000) {
			LOG.fatal("!!! " + capacity + ", " + slot_capacity + ", " + node_load + ", " + node_load_min);
		}
		return cost < 0 ? 0 : cost;
	}

	public Solution solve() {
		LOG.fatal("[Network] solve");

		Map<FlowNetworkTask, FlowNetworkNode> result = mcfn.solve();
		Solution solution = new Solution();
		for (Map.Entry<FlowNetworkTask, FlowNetworkNode> entry : result.entrySet()) {
			FlowNetworkTask task = entry.getKey();
			FlowNetworkNode node = entry.getValue();
			Assignment assignment = new Assignment();
			assignment.setApp(task.app);
			assignment.setNode(node.node);
			assignment.setNumber(1);
			assignment.setPriory(task.priority);
			assignment.setRequest(task.request);
			solution.addAssignment(assignment);
		}

		return solution;
	}

	public void launchTask(FiCaSchedulerNode node, RMContainer container, FiCaSchedulerApp app) {
		LOG.fatal("[Update] launch task");
		LOG.fatal("[Update] launch container " + container.getContainer().getNodeHttpAddress() + " on " + node.getHostName() + " for app " + app.getApplicationAttemptId());
		FlowNetworkNode flowNode = nodeMap.get(node);
		FlowNetworkTask task = convertToTask(container, app);
		flowNode.launchTask(task);
	}

	public void completeTask(FiCaSchedulerNode node, RMContainer container, FiCaSchedulerApp app) {
		LOG.fatal("[Update] complete task");
		LOG.fatal("[Update] complete container " + container.getContainer().getNodeHttpAddress() + " on " + node.getHostName() + " for app " + app.getApplicationAttemptId());
		FlowNetworkNode flowNode = nodeMap.get(node);
		FlowNetworkTask task = convertToTask(container, app);
		flowNode.completeTask(task);
	}

	public FlowNetworkTask convertToTask(RMContainer container, FiCaSchedulerApp app) {
		ResourceRequest request = containerRequestMap.get(container);
		Type type = getTaskType(request.getPriority());
		int capacity = getTaskCapacity(app, type);
		Priority priority = containerRequestMap.get(container).getPriority();
		FlowNetworkTask task = new FlowNetworkTask(type, capacity, 0, priority, app, request);
		return task;
	}

	private Type getTaskType(Priority priority) {
		switch (priority.getPriority()) {
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

	private int getTaskCapacity(FiCaSchedulerApp app, Type type) {
		int capacity = conf.getDefaultCapacity();
		if (type.equals(Type.AppMaster)) {
			capacity = conf.getCapacity(BandwidthAwareSchedulerConfiguration.Type.APPMASTER);
		} else if (type.equals(Type.Reduce)) {
			capacity = conf.getCapacity(BandwidthAwareSchedulerConfiguration.Type.REDUCE);
		} else {
			String appName = rmContext.getRMApps().get(app.getApplicationId()).getName();
			if (appName.contains("TeraSort")) {
				capacity = conf.getCapacity(BandwidthAwareSchedulerConfiguration.Type.TERASORT);
			} else if (appName.contains("word count")) {
				capacity = conf.getCapacity(BandwidthAwareSchedulerConfiguration.Type.WORDCOUNT);
			} else if (appName.contains("grep-search")) {
				capacity = conf.getCapacity(BandwidthAwareSchedulerConfiguration.Type.GREP);
			}
			// LOG.fatal("@ " + app.getApplicationId() + ", " + appName + " -> " + capacity);
		}
		return capacity;
	}

	// --------------------------------

	class FlowNetworkNode {
		// TODO: smart approach here
		final static int MIN_SLOT_MEMORY = 512;
		int capacity;
		int load;
		int slot;
		FiCaSchedulerNode node;
		Map<String, Integer> taskMap = new HashMap<String, Integer>();

		public FlowNetworkNode(FiCaSchedulerNode node, int capacity, int load) {
			this.node = node;
			this.capacity = capacity;
			this.load = load;
			updateSlot();
		}

		public void update() {
			updateSlot();
		}

		public void updateSlot() {
			this.slot = node.getAvailableResource().getMemory() / MIN_SLOT_MEMORY;
			// LOG.fatal("[Slot] update: " + this.slot + " -> " + node.getHostName());
		}

		public void launchTask(FlowNetworkTask task) {
			String taskName = task.getName();
			Integer number = taskMap.get(taskName);
			if (number != null) {
				taskMap.put(taskName, number + 1);
			} else {
				taskMap.put(taskName, 1);
			}
			this.load += task.capacity;
		}

		public void completeTask(FlowNetworkTask task) {
			String taskName = task.getName();
			Integer number = taskMap.get(taskName);
			if (number != null) {
				if (number > 1) {
					taskMap.put(taskName, number - 1);
				} else {
					taskMap.remove(taskName);
				}
				this.load -= task.capacity;
			} else {
				LOG.fatal("[Node] unknown task: " + task.getName());
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((node == null) ? 0 : node.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			return this == obj;
		}

		public String getNodeStatus() {
			StringBuilder builder = new StringBuilder();
			builder.append("Node: " + node.getHostName() + " -> capacity=" + capacity + ", load=" + load + ", slot=" + slot + (taskMap.size() > 0 ? "\n" : ""));
			for (Map.Entry<String, Integer> entry : taskMap.entrySet()) {
				builder.append("\t " + entry.getKey() + ": " + entry.getValue() + "\n");
			}
			return builder.toString();
		}

		@Override
		public String toString() {
			return "Node_" + node.getHostName();
		}

	}

	class FlowNetworkApp {
		FiCaSchedulerApp app;

		public FlowNetworkApp(FiCaSchedulerApp app) {
			this.app = app;
		}

		public void update() {
		}

		public List<FlowNetworkTask> getTasks() {
			List<FlowNetworkTask> tasks = new ArrayList<FlowNetworkTask>();
			for (Priority priority : app.getPriorities()) {
				Type type = getTaskType(priority);
				ResourceRequest request = app.getResourceRequest(priority, FiCaSchedulerNode.ANY);
				int numOfTasks = request.getNumContainers();
				if (numOfTasks > 0) {
					// TODO: get real task capacity
					int capacity = getTaskCapacity(app, type);
					for (int i = 0; i < numOfTasks; i++) {
						FlowNetworkTask task = new FlowNetworkTask(type, capacity, 0, priority, app, request);
						tasks.add(task);
					}
				}
			}
			return tasks;
		}

		public String getInfo() {
			return "";
		}

		public String toString() {
			return "App_" + app.getApplicationId();
		}
	}

	class FlowNetworkTask {

		Type type;
		int capacity;
		int progress;
		Priority priority;
		FiCaSchedulerApp app;
		ResourceRequest request;

		public FlowNetworkTask(Type type, int capacity, int progress, Priority priority, FiCaSchedulerApp app, ResourceRequest request) {
			this.type = type;
			this.capacity = capacity;
			this.progress = progress;
			this.priority = priority;
			this.app = app;
			this.request = request;
		}

		public void update() {
		}

		public String getName() {
			RMApp rmApp = rmContext.getRMApps().get(this.app.getApplicationId());
			String appName = rmApp != null ? rmApp.getName() : "null";
			return "Task_" + app.getApplicationId() + "_" + appName + "_" + type.toString();
		}

		public String getInfo() {
			// TODO: ...
			return "";
		}

		public String toString() {
			return getName();
		}
	}

	class Solution {
		List<Assignment> assignments = new ArrayList<Assignment>();

		public void addAssignment(Assignment assignment) {
			assignments.add(assignment);
		}

		public List<Assignment> getAssignments() {
			return assignments;
		}
	}

	class Assignment {
		FiCaSchedulerApp app;
		FiCaSchedulerNode node;
		ResourceRequest request;
		Priority priory;
		int number;

		public FiCaSchedulerApp getApp() {
			return app;
		}

		public void setApp(FiCaSchedulerApp app) {
			this.app = app;
		}

		public FiCaSchedulerNode getNode() {
			return node;
		}

		public void setNode(FiCaSchedulerNode node) {
			this.node = node;
		}

		public ResourceRequest getRequest() {
			return request;
		}

		public void setRequest(ResourceRequest request) {
			this.request = request;
		}

		public Priority getPriory() {
			return priory;
		}

		public void setPriory(Priority priory) {
			this.priory = priory;
		}

		public int getNumber() {
			return number;
		}

		public void setNumber(int number) {
			this.number = number;
		}

	}

	class MinCostFlowNetwork {
		List<FlowNetworkNode> nodes;
		List<FlowNetworkTask> tasks;
		Store store = null;
		List<IntVar> constraints = null;

		private MinCostFlowNetwork(List<FlowNetworkNode> nodes, List<FlowNetworkTask> tasks) {
			this.nodes = nodes;
			this.tasks = tasks;
		}

		public void createNetwork() {

			LOG.fatal("[MCFN] create min cost flow network");
			int numOfTasks = tasks.size();
			int maxAllowedTasks = 0;
			int expectedLoading = getExpectedCapacity(tasks);
			int minLoading = getMinLoading(nodes);

			for (FlowNetworkNode node : nodes) {
				maxAllowedTasks += node.slot;
			}

			LOG.fatal("[MCFN] numOfTasks: " + numOfTasks);
			LOG.fatal("[MCFN] maxAllowedTasks: " + maxAllowedTasks);
			LOG.fatal("[MCFN] expectedLoading: " + expectedLoading);
			LOG.fatal("[MCFN] minLoading: " + minLoading);

			store = new Store();
			constraints = new ArrayList<IntVar>();

			NetworkBuilder net = new NetworkBuilder();

			Node source = net.addNode("source", numOfTasks);
			Node sink = net.addNode("sink", -numOfTasks);
			Node unscheduledNode = net.addNode("unshceduled", 0);

			Map<FlowNetworkNode, Node> jacop_node_map = new HashMap<FlowNetwork.FlowNetworkNode, Node>();
			for (FlowNetworkNode node : nodes) {
				Node jacop_node = net.addNode(node.toString(), 0);
				jacop_node_map.put(node, jacop_node);
			}

			Map<FlowNetworkTask, Node> jacop_task_map = new HashMap<FlowNetwork.FlowNetworkTask, Node>();
			for (FlowNetworkTask task : tasks) {
				Node taskNode = net.addNode(task.getName(), 0);
				jacop_task_map.put(task, taskNode);
			}

			// Create source->task
			for (FlowNetworkTask task : tasks) {

				Node taskNode = jacop_task_map.get(task);

				// Create source to task node
				IntVar sourceToTask = new IntVar(store, "Source->" + taskNode.name, 1, 1);

				// if (Type.AppMaster.equals(task.type)) {
				// sourceToTask = new IntVar(store, "Source->" + taskNode.name, 3, 3);
				// } else {
				// sourceToTask = new IntVar(store, "Source->" + taskNode.name, 1, 1);
				// }
				net.addArc(source, taskNode, 0, sourceToTask);
				constraints.add(sourceToTask);
			}

			// Create task->computing, task->unscheduled
			for (Map.Entry<FlowNetworkTask, Node> task_entry : jacop_task_map.entrySet()) {

				FlowNetworkTask task = task_entry.getKey();
				Node taskNode = task_entry.getValue();

				for (Map.Entry<FlowNetworkNode, Node> entry : jacop_node_map.entrySet()) {
					Node computingNode = entry.getValue();
					IntVar taskToNode = new IntVar(store, taskNode.name + " -> " + computingNode.name, 0, 1);
					int cost = convertCost(task.capacity, convertSlotCapacity(entry.getKey()), entry.getKey().load, minLoading);
					Arc arc = net.addArc(taskNode, computingNode, cost, taskToNode);
					LOG.fatal("[Arc] " + taskToNode + " -> cost = " + cost);
					constraints.add(taskToNode);
				}

				IntVar taskToUnscheduledNode = new IntVar(store, taskNode.name + " -> " + unscheduledNode.name, 0, 1);
				// TODO: better cost model for unscheduled tasks - task.capacity * 1.2
				net.addArc(taskNode, unscheduledNode, (int) (task.capacity * 1.2), taskToUnscheduledNode);
				constraints.add(taskToUnscheduledNode);

			}

			// Create computing->sink
			for (Map.Entry<FlowNetworkNode, Node> entry : jacop_node_map.entrySet()) {
				Node computingNode = entry.getValue();
				// TODO: smarter lower and upper bound
				IntVar computingToSink = new IntVar(store, computingNode.name + " -> " + sink.name, 0, entry.getKey().slot);
				net.addArc(computingNode, sink, 0, computingToSink);
				constraints.add(computingToSink);
			}

			// Create unscheduled->sink
			int numOfUnscheduledTasks = numOfTasks - getTotalAvailableSlots(nodes);
			numOfUnscheduledTasks = numOfUnscheduledTasks < 0 ? 0 : numOfUnscheduledTasks;
			// numOfUnscheduledTasks = numOfTasks;
			System.out.println("@@ total: " + numOfTasks);
			System.out.println("@@ available: " + getTotalAvailableSlots(nodes));
			System.out.println("@@ unscheduled: " + numOfUnscheduledTasks);
			IntVar unscheduledToSink = new IntVar(store, unscheduledNode.name + " -> " + sink.name, 0, numOfTasks);
			net.addArc(unscheduledNode, sink, 0, unscheduledToSink);
			constraints.add(unscheduledToSink);

			IntVar cost = new IntVar(store, "cost", 0, Integer.MAX_VALUE);
			net.setCostVariable(cost);
			// Done with creating
			store.impose(new NetworkFlow(net));
		}

		private int getTotalAvailableSlots(List<FlowNetworkNode> nodes) {
			int sum = 0;
			for (FlowNetworkNode node : nodes) {
				sum += node.slot;
			}
			return sum;
		}

		private int convertSlotCapacity(FlowNetworkNode node) {
			// TODO: may have synchronization problem
			if (node.slot == 0) {
				return 0;
			}
			int slotCapacity = (int) ((double) (node.capacity - node.load) / (double) (node.slot));
			if (slotCapacity <= 0 || slotCapacity >= 100) {
				LOG.fatal("??? " + node.getNodeStatus());
			}
			return slotCapacity;
		}

		public Map<FlowNetworkTask, FlowNetworkNode> solve() {
			if (conf.getBalancing()) {
				LOG.fatal("[Mode] balancing");
				return constructBalacningAssignment();
			}
			printConstraints();
			IntVar[] vars = new IntVar[constraints.size()];
			constraints.toArray(vars);

			Search<IntVar> label = new DepthFirstSearch<IntVar>();
			Indomain<IntVar> method = new IndomainRandom<IntVar>();
			SelectChoicePoint<IntVar> select = new InputOrderSelect<IntVar>(store, vars, method);
			label.getSolutionListener().searchAll(false);
			label.getSolutionListener().recordSolutions(true);
			label.setTimeOut(3);
			// IntVar minCost = new IntVar(store, "minCost", 0, Integer.MAX_VALUE);
			long time_before = System.currentTimeMillis();
			label.labeling(store, select);
			// label.labeling(store, select, minCost);
			long time_after = System.currentTimeMillis();
			LOG.fatal("[Solver] running Time = " + (time_after - time_before));

			LOG.fatal("[Solver] min cost: " + label.getCostValue());

			int numOfSolutions = label.getSolutionListener().solutionsNo();
			for (int i = 1; i <= numOfSolutions; i++) {
				StringBuilder builder = new StringBuilder("Solution: " + i + ": ");
				for (int j = 0; j < label.getSolution(i).length; j++) {
					builder.append(label.getSolution(i)[j] + ", ");
				}
				LOG.fatal(builder);
			}

			Map<FlowNetworkTask, FlowNetworkNode> assignment;
			if (numOfSolutions > 0) {
				int number = new Random().nextInt(numOfSolutions) + 1;
				LOG.fatal("[Solver] pick solution: " + number);
				assignment = constructSolution(label.getSolution(number));
			} else {
				LOG.fatal("[Solver] no solution found");
				assignment = new HashMap<FlowNetwork.FlowNetworkTask, FlowNetwork.FlowNetworkNode>();
			}
			return assignment;
		}

		private Map<FlowNetworkTask, FlowNetworkNode> constructBalacningAssignment() {
			LOG.fatal("[LoadBalancer] assignment...");
			Map<FlowNetworkTask, FlowNetworkNode> assignment = new HashMap<FlowNetworkTask, FlowNetworkNode>();
			Set<FlowNetworkTask> orderedTasks = new TreeSet<FlowNetworkTask>(new Comparator<FlowNetworkTask>() {

				@Override
				public int compare(FlowNetworkTask arg0, FlowNetworkTask arg1) {
					return arg0.capacity <= arg1.capacity ? -1 : 1;
				}
			});

			orderedTasks.addAll(tasks);
			int index = 0;
			for (FlowNetworkTask task : orderedTasks) {
				FlowNetworkNode node = nodes.get(index++ % nodes.size());
				if (node.slot > 0) {
					assignment.put(task, node);
				}
			}

			return assignment;
		}

		private Map<FlowNetworkTask, FlowNetworkNode> constructSolution(Domain[] result) {

			Map<FlowNetworkTask, FlowNetworkNode> assignment = new HashMap<FlowNetwork.FlowNetworkTask, FlowNetwork.FlowNetworkNode>();

			int numOfTasks = tasks.size();
			int numOfNodes = nodes.size();

			for (int i = 0; i < numOfTasks; i++) {
				IntDomain sourceToTask = (IntDomain) result[i];
				if (sourceToTask.value() == 1) {
					LOG.fatal("[Solver] task " + i + " has been picked");
				}

				int task_to_computing_index = numOfTasks + i * (numOfNodes + 1);
				for (int j = 0; j < numOfNodes; j++) {
					IntDomain taskToComputing = (IntDomain) result[task_to_computing_index + j];
					if (taskToComputing.value() == 1) {
						LOG.fatal("[Solver] task " + i + " has been assigned to node " + j);
						assignment.put(tasks.get(i), nodes.get(j));
					}
				}

				int task_to_unscheduled_index = task_to_computing_index + numOfNodes;
				IntDomain taskToUnscheduled = (IntDomain) result[task_to_unscheduled_index];
				if (taskToUnscheduled.value() == 1) {
					LOG.fatal("[Solver] task " + i + " is not scheduled");
				}
			}

			int computing_to_sink_index = numOfTasks + numOfTasks * (numOfNodes + 1);
			for (int i = 0; i < numOfNodes; i++) {
				IntDomain computingToSink = (IntDomain) result[computing_to_sink_index + i];
				LOG.fatal("[Solver] node " + i + " has been assigned " + computingToSink.value() + " tasks");
			}

			int unscheduled_to_sink_index = result.length - 1;
			IntDomain unscheduledToSink = (IntDomain) result[unscheduled_to_sink_index];
			LOG.fatal("[Solver] unscheduled tasks: " + unscheduledToSink.value());

			return assignment;
		}

		private void printConstraints() {

			LOG.fatal("------------------------------------");
			LOG.fatal("@@ constrains ->");

			for (IntVar var : constraints) {
				LOG.fatal(var);
			}

			LOG.fatal("------------------------------------");
		}
	}

}
