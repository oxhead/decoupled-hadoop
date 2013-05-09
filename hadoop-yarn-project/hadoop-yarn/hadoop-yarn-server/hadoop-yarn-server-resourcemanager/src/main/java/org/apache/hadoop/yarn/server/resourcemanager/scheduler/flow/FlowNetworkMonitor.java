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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
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
		FlowNetworkNode flowNode = new FlowNetworkNode(node, capacity, 0);
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

	public FlowNetworkTask lookupTask(RMContainer container) {
		FlowNetworkTask task = null;
		for (FlowNetworkApp app : appMap.values()) {
			task = app.lookupTask(container);
			if (task != null) {
				return task;
			}
		}
		return task;
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

	public List<FlowNetworkTask> getTasks() {
		List<FlowNetworkTask> unscheduleTasks = new ArrayList<FlowNetworkTask>();

		for (FiCaSchedulerApp app : apps) {
			FlowNetworkApp flowNetworkApp = lookupApp(app);
			unscheduleTasks.addAll(flowNetworkApp.getTasks());
		}
		return unscheduleTasks;
	}

	public List<FlowNetworkTask> getUnscheduleTasks() {
		List<FlowNetworkTask> unscheduleTasks = new ArrayList<FlowNetworkTask>();

		for (FiCaSchedulerApp app : apps) {
			FlowNetworkApp flowNetworkApp = lookupApp(app);
			unscheduleTasks.addAll(flowNetworkApp.getUnscheduleTasks());
		}
		return unscheduleTasks;
	}

	public void launchTask(FiCaSchedulerNode node, RMContainer container, FiCaSchedulerApp app) {
		LOG.fatal("[Update] launch container " + container.getContainer().getNodeHttpAddress() + " on " + node.getHostName() + " for app " + app.getApplicationAttemptId());
		FlowNetworkNode flowNode = lookupNode(node);
		FlowNetworkApp flowApp = lookupApp(app);
		FlowNetworkTask task = flowApp.launchTask(container);
		flowNode.launchTask(task);
		task.node = flowNode;
		if (task.type.equals(Type.Map)) {
			if (task.storage != null) {
				task.storage.launchTask(task);
			}
		}
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
			flowRate = new FlowRate(1, 1);
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
		return request.getCapability().getMemory() / 512;
	}

	// --------------------------------

	class FlowNetworkNode {
		// TODO: smart approach here
		final static int MIN_SLOT_MEMORY = 512;
		double capacity;
		double load;
		int slot;
		FiCaSchedulerNode node;
		// Map<String, Integer> taskMap = new HashMap<String, Integer>();
		List<FlowNetworkTask> launchedTasks = new LinkedList<FlowNetworkTask>();

		public FlowNetworkNode(FiCaSchedulerNode node, double capacity, int load) {
			this.node = node;
			this.capacity = capacity;
			this.load = load;
			updateSlot();
		}

		public void update() {
			updateSlot();
			// updateLoad();
		}

		public void updateSlot() {
			// TODO: handle by FlowNetworkNode? not FiCaSchedulerNode
			this.slot = node.getAvailableResource().getMemory() / MIN_SLOT_MEMORY;
			// LOG.fatal("[Slot] update: " + this.slot + " -> " +
			// node.getHostName());
		}

		// TODO: how to calcuate effice flow rate
		public void updateLoad() {
			int sumOfLoad = 0;
			for (FlowNetworkTask task : launchedTasks) {
				sumOfLoad += task.flowRate.flowIn;
			}
			this.load = sumOfLoad;
		}

		public void launchTask(FlowNetworkTask task) {
			launchedTasks.add(task);
			// should update? will be update periodically
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

		public String getNodeStatus() {
			StringBuilder builder = new StringBuilder();
			builder.append("Node: " + node.getHostName() + " -> capacity=" + capacity + ", load=" + load + ", slot=" + slot + ", launched=" + launchedTasks.size() + "\n");
			for (FlowNetworkTask task : launchedTasks) {
				builder.append("\t launched task: " + task + "\n");
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
		// int numOfLaunchedMap = 0;
		// int numOfLaunchedReduce = 0;

		Map<RMContainer, FlowNetworkTask> containerTaskMap = new HashMap<RMContainer, FlowNetworkTask>();
		private boolean createAppMaster = false;
		private boolean createMap = false;
		private boolean createReduce = false;
		// List<FlowNetworkTask> tasks = new
		// LinkedList<FlowNetworkMonitor.FlowNetworkTask>();
		List<FlowNetworkTask> mapTasks = new LinkedList<FlowNetworkMonitor.FlowNetworkTask>();
		List<FlowNetworkTask> reduceTasks = new LinkedList<FlowNetworkMonitor.FlowNetworkTask>();
		FlowNetworkTask appMasterTask;

		public FlowNetworkApp(FiCaSchedulerApp app) {
			this.app = app;
		}

		public FlowNetworkTask lookupTask(RMContainer container) {
			return containerTaskMap.get(container);
		}

		// create a task after it has been launched
		public FlowNetworkTask launchTask(RMContainer container) {
			Type type = getTaskType(container.getContainer().getPriority());
			for (FlowNetworkTask task : getTasks()) {
				if (!task.launched && task.type.equals(type)) {
					LOG.fatal("<> task launched: " + task);
					task.launched = true;
					containerTaskMap.put(container, task);
					return task;
				}
			}
			return null;
		}

		public FlowNetworkTask completeTask(RMContainer container) {
			FlowNetworkTask task = containerTaskMap.get(container);
			task.completed = true;
			// task.rmContainer = null;
			return task;
		}

		public boolean isTaskTypeCreated(int priority) {
			if (priority == 0) {
				return this.createAppMaster;
			} else if (priority == 10) {
				return this.createReduce;
			} else {
				return this.createMap;
			}
		}

		public boolean hasUnlaunchedTasks() {
			for (FlowNetworkTask task : getTasks()) {
				if (task != null && !task.launched) {
					return true;
				}
			}
			return false;
		}

		public boolean hasIncompletedMapTasks() {
			for (FlowNetworkTask task : getTasks()) {
				if (task != null && task.type.equals(Type.Map) && !task.completed) {
					return true;
				}
			}
			return false;
		}

		public boolean hasIncompletedReduceTasks() {
			for (FlowNetworkTask task : getTasks()) {
				if (task != null && task.type.equals(Type.Reduce) && !task.completed) {
					return true;
				}
			}
			return false;
		}

		public List<FlowNetworkTask> getCompletedMapTasks() {
			List<FlowNetworkTask> completedTasks = new LinkedList<FlowNetworkMonitor.FlowNetworkTask>();
			for (FlowNetworkTask task : mapTasks) {
				if (task.completed) {
					completedTasks.add(task);
				}
			}
			return completedTasks;
		}

		public List<FlowNetworkTask> getCompletedReduceTasks() {
			List<FlowNetworkTask> completedTasks = new LinkedList<FlowNetworkMonitor.FlowNetworkTask>();
			for (FlowNetworkTask task : reduceTasks) {
				if (task.completed) {
					completedTasks.add(task);
				}
			}
			return completedTasks;
		}

		public void rebuildTask() {
			LOG.fatal("[Rebuild] check...");
			for (Priority priority : app.getPriorities()) {
				if (priority.getPriority() == PRIORITY_MAP) {
					if (!hasIncompletedMapTasks()) {
						LOG.fatal("[Rebuild] all map tasks finish");
						ResourceRequest request = app.getResourceRequest(priority, FiCaSchedulerNode.ANY);
						List<FlowNetworkTask> completedReduceTasks = getCompletedMapTasks();
						LOG.fatal("[Rebuild] still requires map containers: " + request.getNumContainers());
						for (int i = 0; i < request.getNumContainers(); i++) {
							FlowNetworkTask task = completedReduceTasks.get(i);
							LOG.fatal("[Rebuild] rebuild map task: " + task);
							task.launched = false;
							task.rmContainer = null;
							task.node = null;
							task.completed = false;
						}
					} else {
						LOG.fatal("[Rebuild] still has incompleted map tasks");
					}
				} else if (priority.getPriority() == PRIORITY_REDUCE) {
					if (!hasIncompletedReduceTasks()) {
						LOG.fatal("[Rebuild] all reduce tasks finish");
						ResourceRequest request = app.getResourceRequest(priority, FiCaSchedulerNode.ANY);
						List<FlowNetworkTask> completedReduceTasks = getCompletedReduceTasks();
						LOG.fatal("[Rebuild] still requires reduce containers: " + request.getNumContainers());
						for (int i = 0; i < request.getNumContainers(); i++) {
							FlowNetworkTask task = completedReduceTasks.get(i);
							LOG.fatal("[Rebuild] rebuild reduce task: " + task);
							task.launched = false;
							task.rmContainer = null;
							task.node = null;
							task.completed = false;
						}
					} else {
						LOG.fatal("[Rebuild] still has incompleted reduce tasks");
					}

				}
			}
		}

		// TODO: Tasks will be created in waves, if containers fail...should fix
		public void update() {
			if (createAppMaster && createMap && createReduce) {
				rebuildTask();
			} else {
				buildTask();
			}
		}

		public void buildTask() {
			for (Priority priority : app.getPriorities()) {
				if (priority.getPriority() == PRIORITY_MAP) {
					if (!createMap) {
						createMap = true;
						Type type = getTaskType(priority);
						FlowRate flowRate = getTaskFlowRate(lookupApp(app), type);
						Map<String, ResourceRequest> requests = app.getResourceRequests(priority);
						if (requests.size() == 3) {
							for (Map.Entry<String, ResourceRequest> entry : requests.entrySet()) {
								String host = entry.getKey();
								ResourceRequest request = entry.getValue();
								if (!host.contains("rack") && !host.contains("*")) {
									LOG.fatal("<> request containers: " + request.getNumContainers());
									for (int i = 0; i < request.getNumContainers(); i++) {
										int slot = getTaskSlot(request);
										FlowNetworkStorage storage = null;
										storage = lookupStorage(host);
										FlowNetworkTask task = new FlowNetworkTask(Type.Map, flowRate, slot, this, storage);
										mapTasks.add(task);
										LOG.fatal("<> add task: " + task);
									}
								}
							}
						} else {
							for (Map.Entry<String, ResourceRequest> entry : requests.entrySet()) {
								String host = entry.getKey();
								ResourceRequest request = entry.getValue();
								if (host.contains("*")) {
									LOG.fatal("<> request containers: " + request.getNumContainers());
									for (int i = 0; i < request.getNumContainers(); i++) {
										int slot = getTaskSlot(request);
										FlowNetworkStorage storage = null;
										FlowNetworkTask task = new FlowNetworkTask(Type.Map, flowRate, slot, this, storage);
										mapTasks.add(task);
										LOG.fatal("<> add task: " + task);
									}
								}
							}
						}
					}
				} else if (priority.getPriority() == PRIORITY_REDUCE) {
					if (!createReduce) {
						createReduce = true;
						Type type = getTaskType(priority);
						FlowRate flowRate = getTaskFlowRate(lookupApp(app), type);
						ResourceRequest request = app.getResourceRequest(priority, FiCaSchedulerNode.ANY);
						for (int i = 0; i < request.getNumContainers(); i++) {
							int slot = getTaskSlot(request);
							FlowNetworkTask task = new FlowNetworkTask(Type.Reduce, flowRate, slot, this, null);
							reduceTasks.add(task);
						}
					}
				} else if (priority.getPriority() == PRIORITY_APPMASTER) {
					if (!createAppMaster) {
						createAppMaster = true;
						Type type = getTaskType(priority);
						FlowRate flowRate = getTaskFlowRate(lookupApp(app), type);
						ResourceRequest request = app.getResourceRequest(priority, FiCaSchedulerNode.ANY);
						int slot = getTaskSlot(request);
						FlowNetworkTask task = new FlowNetworkTask(Type.AppMaster, flowRate, slot, this, null);
						appMasterTask = task;
					}
				} else {
					LOG.fatal("@@@@@@@@@@@@@@@@@@@@@@@@@ fail tasks found");
				}
			}
		}

		public List<FlowNetworkTask> getTasks() {
			List<FlowNetworkTask> tasks = new LinkedList<FlowNetworkMonitor.FlowNetworkTask>();
			tasks.add(appMasterTask);
			tasks.addAll(mapTasks);
			tasks.addAll(reduceTasks);
			return tasks;
		}

		public List<FlowNetworkTask> getUnscheduleTasks() {
			List<FlowNetworkTask> unscheduleTasks = new LinkedList<FlowNetworkMonitor.FlowNetworkTask>();
			for (FlowNetworkTask task : getTasks()) {
				// TODO: why can be null?balancing model
				if (task != null && !task.launched) {
					unscheduleTasks.add(task);
				}
			}
			return unscheduleTasks;
		}

		public String toString() {
			return "App_" + app.getApplicationId();
		}
	}

	class FlowNetworkTask {

		Type type;
		FlowRate flowRate;
		FlowNetworkApp app;
		FlowNetworkNode node;
		FlowNetworkStorage storage;
		int slot; // the number of slots required, calculated by memory
					// requirement
		boolean launched = false;
		boolean completed = false;

		RMContainer rmContainer;

		public FlowNetworkTask(Type type, FlowRate flowRate, int slot, FlowNetworkApp app, FlowNetworkStorage storage) {
			this.type = type;
			this.flowRate = flowRate;
			this.app = app;
			this.slot = slot;
			this.storage = storage;
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

		public int getPriority() {
			if (this.type.equals(Type.AppMaster)) {
				return PRIORITY_APPMASTER;
			} else if (this.type.equals(Type.Map)) {
				return PRIORITY_MAP;
			} else if (this.type.equals(Type.Reduce)) {
				return PRIORITY_REDUCE;
			}
			return -1;
		}

	}

	class FlowNetworkStorage extends FlowNetworkPoint {
		List<FlowNetworkTask> servingTasks = new LinkedList<FlowNetworkTask>();
		String host;
		Map<FlowNetworkApp, Integer> dataBlockMap = new HashMap<FlowNetworkMonitor.FlowNetworkApp, Integer>();

		public FlowNetworkStorage(String host) {
			this.host = host;
		}

		void launchTask(FlowNetworkTask task) {
			servingTasks.add(task);
			update();
		}

		void completeTask(FlowNetworkTask task) {
			servingTasks.remove(task);
			update();
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

}
