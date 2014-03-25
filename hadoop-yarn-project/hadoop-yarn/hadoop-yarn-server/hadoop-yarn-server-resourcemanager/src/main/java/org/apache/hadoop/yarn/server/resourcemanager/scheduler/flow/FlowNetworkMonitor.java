/**
 * 
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkTask;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowSchedulerConfiguration.Job;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.Task.Type;

/**
 * 
 * This class is used to maintain the flow usage over the entire network. It
 * updates the network status when tasks are done or nodes have update. These
 * changes are notified by FlowScheduler.
 * 
 */
public class FlowNetworkMonitor {

	private static final Log LOG = LogFactory.getLog(FlowNetworkMonitor.class);

	public static final int PRIORITY_APPMASTER = 0;
	public static final int PRIORITY_MAP = 20;
	public static final int PRIORITY_REDUCE = 10;

	RMContext rmContext;
	FlowSchedulerConfiguration conf;

	Map<FiCaSchedulerNode, FlowNetworkNode> nodeMap = new HashMap<FiCaSchedulerNode, FlowNetworkNode>();
	Map<FiCaSchedulerApp, FlowNetworkApp> appMap = new HashMap<FiCaSchedulerApp, FlowNetworkApp>();
	List<FiCaSchedulerApp> apps = new LinkedList<FiCaSchedulerApp>();
	Map<String, FlowNetworkStorage> storageMap = new HashMap<String, FlowNetworkStorage>();

	public FlowNetworkMonitor(RMContext rmContext, FlowSchedulerConfiguration conf) {
		this.rmContext = rmContext;
		this.conf = conf;
	}

	public void addNode(FiCaSchedulerNode node) {
		LOG.fatal("[Network] add node: " + node.getHostName());
		double capacity = conf.getNodeCapacity(node.getHostName());
		FlowNetworkNode flowNode = new FlowNetworkNode(node, capacity);
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

		String appName = rmContext.getRMApps().get(app.getApplicationId()).getName();
		Job job;
		if (appName.contains("TeraSort")) {
			job = FlowSchedulerConfiguration.Job.TERASORT;
		} else if (appName.contains("word count")) {
			job = FlowSchedulerConfiguration.Job.WORDCOUNT;
		} else if (appName.contains("grep-search")) {
			job = FlowSchedulerConfiguration.Job.GREP;
		} else {
			job = FlowSchedulerConfiguration.Job.DEFAULT;
		}

		FlowRate mapFlowRate = conf.getFlowRate(job, Type.Map);
		FlowRate reduceFlowRate = conf.getFlowRate(job, Type.Reduce);
		FlowNetworkApp flowApp = new FlowNetworkApp(app, mapFlowRate, reduceFlowRate);

		appMap.put(app, flowApp);
		apps.add(app);
	}

	public void removeApp(FiCaSchedulerApp app) {
		LOG.fatal("[Network] remove app: " + app.getApplicationAttemptId());
		appMap.remove(app);
		apps.remove(app);
	}

	public FlowNetworkNode lookupNode(FiCaSchedulerNode node) {
		return nodeMap.get(node);
	}

	public FlowNetworkStorage randomStorage() {
		List<FlowNetworkStorage> storageNodes = new ArrayList<FlowNetworkMonitor.FlowNetworkStorage>(storageMap.values());
		return storageNodes.get(new Random().nextInt(storageNodes.size()));
	}

	public FlowNetworkStorage lookupStorage(String host) {
		if (!storageMap.containsKey(host)) {
			FlowNetworkStorage storage = new FlowNetworkStorage(host);
			// TODO: defined here
			storage.capacity = 80;
			storageMap.put(host, storage);
		}
		return storageMap.get(host);
	}

	public FlowNetworkApp lookupApp(FiCaSchedulerApp app) {
		return appMap.get(app);
	}

	public void update(int time) {
		LOG.fatal("[Network] update with time: " + time);

		for (FlowNetworkApp app : appMap.values()) {
			app.update();
		}

		for (FlowNetworkNode node : nodeMap.values()) {
			node.update();
		}

		for (FlowNetworkStorage storage : storageMap.values()) {
			storage.update();
		}

	}

	public void printNodeStatus() {
		for (FlowNetworkNode node : nodeMap.values()) {
			LOG.fatal(node.getNodeResourceStatus());
		}
		for (FlowNetworkNode node : nodeMap.values()) {
			LOG.fatal(node.getNodeStatus());
		}
	}

	public List<FlowNetworkNode> getNodes() {
		List<FlowNetworkNode> nodes = new ArrayList<FlowNetworkMonitor.FlowNetworkNode>();
		nodes.addAll(nodeMap.values());
		return nodes;
	}

	public List<FlowNetworkStorage> getStorageNodes() {
		return new LinkedList<FlowNetworkMonitor.FlowNetworkStorage>(storageMap.values());
	}

	public void launchTask(FiCaSchedulerNode node, FiCaSchedulerApp app, RMContainer container) {
		LOG.fatal("[Update] launch container " + container.getContainer().getNodeHttpAddress() + " on " + node.getHostName() + " for app " + app.getApplicationAttemptId());
		FlowNetworkNode flowNode = lookupNode(node);
		FlowNetworkApp flowApp = lookupApp(app);
		FlowNetworkTask task = flowApp.launchTask(container);
		task.node = flowNode;
		flowNode.launchTask(task);
		// no use now
		// if (task.type.equals(Type.Map)) {
		// if (task.storage != null) {
		// task.storage.launchTask(task);
		// }
		// }
	}

	public void completeTask(FiCaSchedulerNode node, RMContainer container, FiCaSchedulerApp app) {
		LOG.fatal("[Update] complete container " + container.getContainer().getNodeHttpAddress() + " on " + node.getHostName() + " for app " + app.getApplicationAttemptId());
		FlowNetworkNode flowNode = lookupNode(node);
		FlowNetworkApp flowApp = lookupApp(app);
		FlowNetworkTask task = flowApp.lookupTask(container);
		flowApp.completeTask(container);
		flowNode.completeTask(task);
		if (task.type.equals(Type.Map)) {
			if (task.storage != null) {
				task.storage.completeTask(task);
			}
		}
	}

	public Type getTaskType(Priority priority) {
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

	private FlowRate getTaskFlowRate(FlowNetworkApp app, Type type) {
		FlowRate flowRate;
		if (type.equals(Type.AppMaster)) {
			flowRate = new FlowRate(0, 0);
		} else {
			String appName = rmContext.getRMApps().get(app.app.getApplicationId()).getName();
			if (appName.contains("TeraSort")) {
				flowRate = conf.getFlowRate(FlowSchedulerConfiguration.Job.TERASORT, type);
			} else if (appName.contains("word count")) {
				flowRate = conf.getFlowRate(FlowSchedulerConfiguration.Job.WORDCOUNT, type);
			} else if (appName.contains("grep-search")) {
				flowRate = conf.getFlowRate(FlowSchedulerConfiguration.Job.GREP, type);
			} else {
				flowRate = conf.getFlowRate(FlowSchedulerConfiguration.Job.DEFAULT, type);
			}
		}
		return flowRate;
	}

	private int getTaskSlot(ResourceRequest request) {
		// TODO: should be configurable
		// TODO: make sure this change could cause a bug?
		return (int) (Math.ceil(request.getCapability().getMemory()) / 512);
	}

	// --------------------------------

	class FlowNetworkNode {

		// TODO: smart approach here
		private final static int MIN_SLOT_MEMORY = 1024;
		double capacity;
		FiCaSchedulerNode node;
		List<FlowNetworkTask> launchedTasks = new LinkedList<FlowNetworkTask>();

		private double effective_load = 0;
		private double slot_capability = 0;

		public FlowNetworkNode(FiCaSchedulerNode node, double capacity) {
			this.node = node;
			this.capacity = capacity;
		}

		public void update() {
			this.effective_load = calculateEffectiveLoad();
			this.slot_capability = calculateSlotCapacity();
		}

		public int getAvailableSlots() {
			return node.getAvailableResource().getMemory() / MIN_SLOT_MEMORY;
		}

		public void launchTask(FlowNetworkTask task) {
			launchedTasks.add(task);
			update();
		}

		public void completeTask(FlowNetworkTask task) {
			launchedTasks.remove(task);
			update();
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

		public String getNodeResourceStatus() {
			return "[#] Node: " + node.getHostName() + " -> running: " + node.getRunningContainers().size() + ", used: " + node.getUsedResource() + ", available: " + node.getAvailableResource();
		}

		public String getNodeStatus() {
			StringBuilder builder = new StringBuilder();
			builder.append("[*] Node: " + node.getHostName() + " -> capacity=" + capacity + ", load=" + getEffectiveLoad() + ", slot_capacity=" + getSlotCapability() + ", slot="
					+ this.getAvailableSlots() + ", launched=" + launchedTasks.size() + "\n");
			for (FlowNetworkTask task : launchedTasks) {
				builder.append("\t launched task: " + task + "\n");
			}
			return builder.toString();
		}

		@Override
		public String toString() {
			return "Node_" + node.getHostName();
		}

		public double getEffectiveLoad() {
			return this.effective_load;
		}

		public double getSlotCapability() {
			return this.slot_capability;
		}

		public double getLoad() {
			double sumOfLoad = 0;
			for (FlowNetworkTask task : this.launchedTasks) {
				sumOfLoad += task.flowRate.flowIn;
			}
			return sumOfLoad;
		}

		// non-linear load calculation CPU-bound vs. network-bound
		private double calculateEffectiveLoad() {
			double sumOfLoad = 0;
			for (FlowNetworkTask task : this.launchedTasks) {
				sumOfLoad += task.flowRate.flowIn;
			}
			List<Double> numbers = new LinkedList<Double>();
			for (FlowNetworkTask taskInNode : this.launchedTasks) {
				numbers.add(taskInNode.flowRate.flowIn);
			}
			double std = (int) MathUtil.calcuateStd(numbers);
			double facotr = 0.1;
			double effective_load = sumOfLoad - facotr * std;
			return effective_load;
		}

		private double calculateSlotCapacity() {
			slot_capability = 0;
			if (getAvailableSlots() != 0) {
				slot_capability = (this.capacity - getEffectiveLoad()) / getAvailableSlots();
			}
			return slot_capability;
		}

	}

	class FlowNetworkApp {
		FiCaSchedulerApp app;
		FlowRate mapFlowRate, reduceFlowRate;
		Map<RMContainer, FlowNetworkTask> launchedTaskMap = new HashMap<RMContainer, FlowNetworkMonitor.FlowNetworkTask>();

		int numOfMapTasks = 0;
		int numOfReduceTasks = 0;

		public FlowNetworkApp(FiCaSchedulerApp app, FlowRate mapFlowRate, FlowRate reduceFlowRate) {
			this.app = app;
			this.mapFlowRate = mapFlowRate;
			this.reduceFlowRate = reduceFlowRate;
		}

		private FlowRate getFlowRate(Type type) {
			if (type.equals(Type.Map)) {
				return mapFlowRate;
			} else if (type.equals(Type.Reduce)) {
				return reduceFlowRate;
			} else if (type.equals(Type.AppMaster)) {
				return new FlowRate(1, 1);
			}
			return conf.getFlowRate(Job.DEFAULT, type);
		}

		public void updateNumOfMapTasks(ResourceRequest request) {
			this.numOfMapTasks = request.getNumContainers() > this.numOfMapTasks ? request.getNumContainers() : this.numOfMapTasks;
		}

		public void updateNumOfReduceTasks(ResourceRequest request) {
			this.numOfReduceTasks = request.getNumContainers() > this.numOfReduceTasks ? request.getNumContainers() : this.numOfReduceTasks;
		}

		public int getNumOfMapTasks() {
			return this.numOfMapTasks;
		}

		public int getNumOfReduceTasks() {
			return this.numOfReduceTasks;
		}

		public List<FlowNetworkTask> getMapTasks() {
			List<FlowNetworkTask> tasks = new LinkedList<FlowNetworkMonitor.FlowNetworkTask>();
			for (FlowNetworkTask task : launchedTaskMap.values()) {
				if (task.type.equals(Type.Map)) {
					tasks.add(task);
				}
			}
			return tasks;
		}

		public List<FlowNetworkTask> getReduceTasks() {
			List<FlowNetworkTask> tasks = new LinkedList<FlowNetworkMonitor.FlowNetworkTask>();
			for (FlowNetworkTask task : launchedTaskMap.values()) {
				if (task.type.equals(Type.Reduce)) {
					tasks.add(task);
				}
			}
			return tasks;
		}

		public FlowNetworkTask lookupTask(RMContainer container) {
			return launchedTaskMap.get(container);
		}

		public FlowNetworkTask launchTask(RMContainer container) {
			Type type = getTaskType(container.getContainer().getPriority());
			int slot = getContainerSlot(container);
			FlowRate flowRate = getFlowRate(type);
			// Don't have the information of storage
			FlowNetworkTask task = new FlowNetworkTask(type, flowRate, slot, this, null, container.getContainer().getPriority());
			task.rmContainer = container;
			this.launchedTaskMap.put(container, task);
			return task;
		}

		public FlowNetworkTask completeTask(RMContainer container) {
			return lookupTask(container);
		}

		public void update() {
		}

		public String toString() {
			return "App_" + app.getApplicationId();
		}
	}

	class FlowNetworkTask {

		Type type;
		FlowRate flowRate;
		int slot = 0;
		// back reference
		FlowNetworkApp app;
		FlowNetworkNode node;
		FlowNetworkStorage storage;

		RMContainer rmContainer;
		Priority priority;

		public FlowNetworkTask(Type type, FlowRate flowRate, int slot, FlowNetworkApp app, FlowNetworkStorage storage, Priority priority) {
			this.type = type;
			this.flowRate = flowRate;
			this.slot = slot;
			this.app = app;
			this.storage = storage;
			this.priority = priority;
		}

		public void update() {
		}

		public String getName() {
			RMApp rmApp = rmContext.getRMApps().get(this.app.app.getApplicationId());
			String appName = rmApp != null ? rmApp.getName() : "null";
			return "Task_" + app.app.getApplicationId() + "_" + appName + "_" + type.toString();
		}

		public String toString() {
			return getName();
		}

		public Priority getPriority() {
			return priority;
		}

	}

	class FlowNetworkStorage extends FlowNetworkPoint {
		List<FlowNetworkTask> servingTasks = new LinkedList<FlowNetworkTask>();
		String host;
		Map<FlowNetworkApp, Integer> dataBlockMap = new HashMap<FlowNetworkMonitor.FlowNetworkApp, Integer>();

		public FlowNetworkStorage(String host) {
			this.host = host;
		}

		// no use now
		void launchTask(FlowNetworkTask task) {
			// servingTasks.add(task);
			// update();
		}

		// no use now
		void completeTask(FlowNetworkTask task) {
			// servingTasks.remove(task);
			// update();
		}

		void update() {
			this.load = 0;
			for (FlowNetworkTask task : servingTasks) {
				this.load += task.flowRate.flowIn;
			}
		}

		@Override
		public String toString() {
			return "FlowNetworkStorage [host=" + host + ", capacity=" + capacity + "]";
		}
	}

	class FlowNetworkRack {
		int load;
		Set<FlowNetworkPoint> nodes = new HashSet<FlowNetworkPoint>();

		void addNode(FlowNetworkPoint node) {
			nodes.add(node);
		}

		boolean isInRack(FlowNetworkPoint node) {
			return nodes.contains(node);
		};
	}

	class FlowNetworkTopology {
		Set<FlowNetworkRack> racks = new HashSet<FlowNetworkRack>();

		void addRack(FlowNetworkRack rack) {
			racks.add(rack);
		}

		boolean isInSameRack(FlowNetworkPoint node1, FlowNetworkPoint node2) {
			for (FlowNetworkRack rack : racks) {
				if (rack.isInRack(node1) && rack.isInRack(node2)) {
					return true;
				}
			}
			return false;
		}
	}

	class FlowNetworkPoint {
		int capacity;
		int load;
	}

	public List<FlowNetworkTask> buildUnscheduledTasks(List<FiCaSchedulerApp> launchedApps) {
		List<FlowNetworkTask> unscheduledTasks = new LinkedList<FlowNetworkMonitor.FlowNetworkTask>();
		for (FiCaSchedulerApp launchedApp : launchedApps) {

			FlowNetworkApp myApp = lookupApp(launchedApp);

			for (Priority priority : launchedApp.getPriorities()) {
				Type type = getTaskType(priority);
				FlowRate flowRate = getTaskFlowRate(myApp, type);
				if (type.equals(Type.Map)) {
					Map<String, ResourceRequest> requests = launchedApp.getResourceRequests(priority);
					myApp.updateNumOfMapTasks(requests.get("*"));
					if (requests.size() == 3) { // normal jobs with map and
												// reduce tasks
						ResourceRequest request = launchedApp.getResourceRequest(priority, FiCaSchedulerNode.ANY);
						myApp.updateNumOfMapTasks(request);
						for (int i = 0; i < request.getNumContainers(); i++) {
							int slot = getTaskSlot(request);
							FlowNetworkStorage storage = null;
							// TODO: combine the modificaiton of prefetching
							// hava a big problem here...
							// storage = lookupStorage(host);
							// storage = randomStorage();
							// TODO: null for quick fix
							FlowNetworkTask task = new FlowNetworkTask(Type.Map, flowRate, slot, myApp, null, priority);
							unscheduledTasks.add(task);
						}

						/**
						 * for (Map.Entry<String, ResourceRequest> entry :
						 * requests.entrySet()) { String host = entry.getKey();
						 * ResourceRequest request = entry.getValue(); // for
						 * data locality if (!host.contains("rack") &&
						 * !host.contains("*")) { for (int i = 0; i <
						 * request.getNumContainers(); i++) { int slot =
						 * getTaskSlot(request); FlowNetworkStorage storage =
						 * null; storage = lookupStorage(host); FlowNetworkTask
						 * task = new FlowNetworkTask(Type.Map, flowRate, slot,
						 * myApp, storage); unscheduledTasks.add(task); } } }
						 */
					} else { // this happens when no need of data from data node
						ResourceRequest request = launchedApp.getResourceRequest(priority, FiCaSchedulerNode.ANY);
						myApp.updateNumOfMapTasks(request);
						for (int i = 0; i < request.getNumContainers(); i++) {
							int slot = getTaskSlot(request);
							FlowNetworkTask task = new FlowNetworkTask(Type.Map, flowRate, slot, myApp, null, priority);
							unscheduledTasks.add(task);
						}
					}

				} else if (type.equals(Type.Reduce)) {
					ResourceRequest request = launchedApp.getResourceRequest(priority, FiCaSchedulerNode.ANY);
					myApp.updateNumOfReduceTasks(request);
					for (int i = 0; i < request.getNumContainers(); i++) {
						int slot = getTaskSlot(request);
						FlowNetworkTask task = new FlowNetworkTask(Type.Reduce, flowRate, slot, myApp, null, priority);
						unscheduledTasks.add(task);
					}
				} else if (type.equals(Type.AppMaster)) {
					ResourceRequest request = launchedApp.getResourceRequest(priority, FiCaSchedulerNode.ANY);
					if (request.getNumContainers() == 1) {
						int slot = getTaskSlot(request);
						FlowNetworkTask task = new FlowNetworkTask(Type.AppMaster, flowRate, slot, myApp, null, priority);
						unscheduledTasks.add(task);
					}
				} else if (type.equals(Type.UNKNOWN)) {
					ResourceRequest request = launchedApp.getResourceRequest(priority, FiCaSchedulerNode.ANY);
					LOG.fatal("@@ unknown task priority: " + priority + "->" + request.getCapability() + ", " + request.getNumContainers());
					// myApp.updateNumOfMapTasks(request);
					for (int i = 0; i < request.getNumContainers(); i++) {
						int slot = getTaskSlot(request);
						FlowNetworkTask task = new FlowNetworkTask(Type.Map, flowRate, slot, myApp, null, priority);
						unscheduledTasks.add(task);
					}
				}
			}
		}
		return unscheduledTasks;
	}

	private int getContainerSlot(RMContainer container) {
		return (int) Math.ceil(container.getContainer().getResource().getMemory() / 512);
	}

}
