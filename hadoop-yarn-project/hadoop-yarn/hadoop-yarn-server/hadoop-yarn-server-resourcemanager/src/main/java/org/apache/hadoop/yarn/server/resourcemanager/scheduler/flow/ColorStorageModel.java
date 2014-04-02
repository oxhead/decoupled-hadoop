package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ColorStorageModel implements AssignmentModel {

	private static final Log LOG = LogFactory.getLog(ColorStorageModel.class);

	List<FlowSchedulerNode> nodes;
	List<FlowSchedulerTask> tasks;
	List<FlowSchedulerStorage> storages;
	List<Assignment> assignments = new LinkedList<Assignment>();

	NodeStore nodeStore;
	StorageStore storageStore;

	AppMasterTaskStore appMasterTaskStore;
	MapTaskStore mapTaskStore;
	ReduceTaskStore reduceTaskStore;

	Map<FlowSchedulerApp, Integer> mapTaskRemain;
	Map<FlowSchedulerApp, Integer> maxReduceTask;

	public ColorStorageModel(List<FlowSchedulerNode> nodes, List<FlowSchedulerTask> tasks, List<FlowSchedulerStorage> storages) {
		this.nodes = nodes;
		this.tasks = tasks;
		this.storages = storages;
	}

	public void construct() {
		appMasterTaskStore = new AppMasterTaskStore();
		mapTaskStore = new MapTaskStore();
		reduceTaskStore = new ReduceTaskStore();
		nodeStore = new NodeStore();
		storageStore = new StorageStore();

		mapTaskRemain = new HashMap<FlowSchedulerApp, Integer>();
		maxReduceTask = new HashMap<FlowSchedulerApp, Integer>();

		for (FlowSchedulerNode node : nodes) {
			nodeStore.addNode(node);
		}
		nodeStore.update();

		for (FlowSchedulerTask task : tasks) {
			if (task.isMap()) {
				if (task.getStorage() != null) {
					storageStore.addTask(task);
				} else {
					mapTaskStore.addTask(task);
				}

				if (!mapTaskRemain.containsKey(task.getApp())) {
					mapTaskRemain.put(task.getApp(), new Integer(0));
				}
				Integer count = mapTaskRemain.get(task.getApp());
				mapTaskRemain.put(task.getApp(), new Integer(count + 1));
			} else if (task.isReduce()) {
				reduceTaskStore.addTask(task);
			} else {
				appMasterTaskStore.addTask(task);
			}
		}
		appMasterTaskStore.update();
		mapTaskStore.update();
		reduceTaskStore.update();
		storageStore.update();

		// calculate the better number of reduce tasks to be scheduled
		for (FlowSchedulerApp app : mapTaskRemain.keySet()) {
			int mapCount = mapTaskRemain.get(app);
			maxReduceTask.put(app, mapCount / 16 + 1);
		}
	}

	public void report() {
		nodeStore.report();
		storageStore.report();
		appMasterTaskStore.report();
		mapTaskStore.report();
		reduceTaskStore.report();
	}

	// TODO: here might be wrong, should assign from larger slot?
	public void solve() {
		assignments.clear();

		LOG.fatal("[before]------------------------");
		report();
		LOG.fatal("[before]------------------------");

		// evenly distribution should be enough
		// nodeStore should not update for even distribution
		while (nodeStore.hasNext() && appMasterTaskStore.hasNext()) {
			ColorComputingNode node = nodeStore.next();
			ColorTask task = appMasterTaskStore.next();
			node.assignTask(task);
			Assignment assignment = new Assignment(task.task, node.node);
			assignments.add(assignment);
			node.assignTask(task);
			LOG.fatal("@@ assign appMaster: " + task.task.getAttemptId() + " to " + node.node.getHostName());
		}

		while (nodeStore.hasNext() && reduceTaskStore.hasNext()) {
			ColorComputingNode node = nodeStore.next();
			ColorTask task = reduceTaskStore.next();
			int maxReduceCount = maxReduceTask.containsKey(task.task.getApp()) ? maxReduceTask.get(task.task.getApp()) : Integer.MAX_VALUE;
			if (maxReduceCount > 0) {
				Assignment assignment = new Assignment(task.task, node.node);
				assignments.add(assignment);
				node.assignTask(task);
				nodeStore.update();
				if (maxReduceTask.containsKey(task.task.getApp())) {
					maxReduceTask.put(task.task.getApp(), maxReduceCount - 1);
				}
				LOG.fatal("@@ assign reduce: " + task.task.getAttemptId() + " to " + node.node.getHostName());
			}
		}

		// TODO: why too many tasks are added
		while (nodeStore.hasNext() && storageStore.hasNext()) {
			ColorComputingNode node = nodeStore.next();
			ColorTask task = storageStore.next();
			Assignment assignment = new Assignment(task.task, node.node);
			assignments.add(assignment);
			node.assignTask(task);
			nodeStore.update();
			storageStore.assignTask(task);
			storageStore.update();

			LOG.fatal("@@ assign map with data: " + task.task.getAttemptId() + " to " + node.node.getHostName());

		}

		while (nodeStore.hasNext() && mapTaskStore.hasNext()) {
			ColorComputingNode node = nodeStore.next();
			ColorTask task = mapTaskStore.next();
			Assignment assignment = new Assignment(task.task, node.node);
			assignments.add(assignment);
			node.assignTask(task);
			LOG.fatal("@@ assign map without data: " + task.task.getAttemptId() + " to " + node.node.getHostName());
			nodeStore.update();
		}

		LOG.fatal("[after]------------------------");
		report();
		LOG.fatal("[after]------------------------");
	}

	@Override
	public List<Assignment> constructAssignments() {
		return assignments;
	}

	// inner classes for color model------------------------

	class AppMasterTaskStore {

		Comparator<ColorTask> appMasterComparator = new Comparator<ColorStorageModel.ColorTask>() {

			@Override
			public int compare(ColorTask o1, ColorTask o2) {
				int result = o1.task.getApp().getApplicationId().compareTo(o2.task.getApp().getApplicationId());
				if (result == 0) {
					return new Integer(o1.task.hashCode()).compareTo(new Integer(o2.hashCode()));
				} else {
					return result;
				}
			}
		};

		private final Log LOG = LogFactory.getLog(AppMasterTaskStore.class);
		PriorityQueue<ColorTask> taskQueue = new PriorityQueue<ColorTask>(100, appMasterComparator);

		public AppMasterTaskStore() {
		}

		public AppMasterTaskStore(List<FlowSchedulerTask> tasks) {
			this.addTasks(tasks);
			update();
		}

		public void addTasks(Iterable<FlowSchedulerTask> tasks) {
			for (FlowSchedulerTask task : tasks) {
				addTask(task);
			}
		}

		public void addTask(FlowSchedulerTask task) {
			if (!task.isAppMaster()) {
				return;
			}
			ColorTask colorTask = new ColorTask(task);
			this.taskQueue.add(colorTask);

		}

		public void update() {
		}

		public boolean hasNext() {
			return taskQueue.size() > 0;
		}

		public ColorTask next() {
			return taskQueue.poll();
		}

		public void report() {
			LOG.fatal("AppMaster=" + taskQueue.size());
			for (ColorTask task : taskQueue) {
				LOG.fatal("\tAppMaster: " + task.task.getAttemptId() + ", map=" + task.task.getApp().getNumOfMapTasks() + ", reduce=" + task.task.getApp().getNumOfReduceTasks());
			}
		}
	}

	class MapTaskStore {

		private final Log LOG = LogFactory.getLog(MapTaskStore.class);
		Map<Double, TreeSet<ColorTask>> store = new HashMap<Double, TreeSet<ColorTask>>();

		PriorityQueue<Double> currentHeap = new PriorityQueue<Double>();
		Double currentKey;

		Comparator<ColorTask> mapComparator = new Comparator<ColorTask>() {

			@Override
			public int compare(ColorTask o1, ColorTask o2) {
				return o1.task.getAttemptId().compareTo(o2.task.getAttemptId());
			}
		};

		public MapTaskStore() {
		}

		public void addTask(FlowSchedulerTask task) {
			ColorTask colorTask = new ColorTask(task);
			if (!store.containsKey(colorTask.demand)) {
				store.put(colorTask.demand, new TreeSet<ColorTask>(mapComparator));
			}
			TreeSet<ColorTask> set = store.get(colorTask.demand);
			set.add(colorTask);

		}

		public void update() {
			PriorityQueue<Double> heap = new PriorityQueue<Double>();
			Iterator<Double> keyIterator = store.keySet().iterator();
			List<Double> removedKey = new LinkedList<Double>();
			while (keyIterator.hasNext()) {
				Double key = keyIterator.next();
				if (store.get(key).size() > 0) {
					heap.add(key);
				} else {
					removedKey.add(key);
				}
			}
			currentHeap = heap;
			for (Double key : removedKey) {
				store.remove(key);
			}
		}

		public boolean hasNext() {
			if (currentHeap.size() == 0) {
				update();
			}
			return currentHeap.size() > 0;
		}

		public ColorTask next() {
			if (currentHeap.size() == 0) {
				update();
			}
			Double key = currentHeap.poll();
			if (key != null) {
				if (store.get(key).size() > 0) {
					ColorTask task = store.get(key).pollFirst();
					return task;
				}
			}
			return null;
		}

		public void report() {
			LOG.fatal("Task store: size=" + store.size());
			for (Entry<Double, TreeSet<ColorTask>> entry : this.store.entrySet()) {
				LOG.fatal("\tdemand=" + entry.getKey() + ", size=" + entry.getValue().size());
			}
		}
	}

	class ReduceTaskStore {

		private final Log LOG = LogFactory.getLog(ReduceTaskStore.class);
		Comparator<FlowSchedulerApp> appComparator = new Comparator<FlowSchedulerApp>() {

			@Override
			public int compare(FlowSchedulerApp o1, FlowSchedulerApp o2) {
				return o1.getApplicationId().compareTo(o2.getApplicationId());
			}
		};
		Comparator<ColorTask> reduceComparator = new Comparator<ColorTask>() {

			@Override
			public int compare(ColorTask o1, ColorTask o2) {
				int result = o1.task.getApp().getApplicationId().compareTo(o2.task.getApp().getApplicationId());
				if (result == 0) {
					return o1.task.getAttemptId().compareTo(o2.task.getAttemptId());
				}
				return result;
			}
		};

		Map<FlowSchedulerApp, TreeSet<ColorTask>> appReduceMap = new HashMap<FlowSchedulerApp, TreeSet<ColorTask>>();

		public ReduceTaskStore() {

		}

		public void update() {
		}

		public void report() {
			LOG.fatal("Reduce store: tasks=" + appReduceMap.size());
			for (Map.Entry<FlowSchedulerApp, TreeSet<ColorTask>> entry : appReduceMap.entrySet()) {
				LOG.fatal("\t" + entry.getKey().getApplicationId() + "=" + entry.getValue().size());
				for (ColorTask task : entry.getValue()) {
					LOG.fatal("\t\t reduce=" + task.task.getAttemptId() + " -> demand=" + task.task.getFlowDemand());
				}
			}
		}

		public void addTask(FlowSchedulerTask task) {
			ColorTask colorTask = new ColorTask(task);
			if (!appReduceMap.containsKey(colorTask.task.getApp())) {
				appReduceMap.put(colorTask.task.getApp(), new TreeSet<ColorTask>(reduceComparator));
			}
			appReduceMap.get(colorTask.task.getApp()).add(colorTask);
		}

		public boolean hasNext() {
			for (Map.Entry<FlowSchedulerApp, TreeSet<ColorTask>> entry : appReduceMap.entrySet()) {
				if (entry.getValue() != null && entry.getValue().size() > 0) {
					return true;
				}
			}
			return false;
		}

		public ColorTask next() {
			TreeSet<FlowSchedulerApp> orderedSet = new TreeSet<FlowSchedulerApp>(appComparator);
			orderedSet.addAll(appReduceMap.keySet());
			FlowSchedulerApp appKey = orderedSet.pollFirst();
			TreeSet<ColorTask> reduceSet = appReduceMap.get(appKey);
			ColorTask task = reduceSet.pollFirst();
			if (reduceSet.size() == 0) {
				appReduceMap.remove(appKey);
			}
			return task;
		}
	}

	// -----------------------------

	class StorageStore {

		private final Log LOG = LogFactory.getLog(StorageStore.class);

		Comparator<ColorStorageNode> storageComparator = new Comparator<ColorStorageNode>() {

			@Override
			public int compare(ColorStorageNode arg0, ColorStorageNode arg1) {
				int result = new Double(arg0.loading).compareTo(new Double(arg1.loading));
				if (result == 0) {
					return new Integer(arg0.hashCode()).compareTo(new Integer(arg1.hashCode()));
				}
				return result;
			}
		};

		PriorityQueue<ColorStorageNode> heap = new PriorityQueue<ColorStorageNode>(100, storageComparator);
		Map<String, ColorStorageNode> hostStorageMap = new HashMap<String, ColorStorageNode>();

		public void addTask(FlowSchedulerTask task) {
			if (!task.isMap()) {
				return;
			}
			if (task.getStorage() == null) {
				return;
			}
			if (!hostStorageMap.containsKey(task.getStorage().getHost())) {
				hostStorageMap.put(task.getStorage().getHost(), new ColorStorageNode(task.getStorage()));
			}
			ColorStorageNode storageNode = hostStorageMap.get(task.getStorage().getHost());
			storageNode.addTask(task);
		}

		public void assignTask(ColorTask task) {
			hostStorageMap.get(task.task.getStorage().getHost()).assignTask(task);
		}

		public void report() {
			LOG.fatal("Storage store: nodes=" + hostStorageMap.size());
			for (ColorStorageNode node : hostStorageMap.values()) {
				LOG.fatal("\tstorage: " + node.node.getHost() + ", loading=" + node.loading);
			}
		}

		public boolean hasNext() {
			for (ColorStorageNode storageNode : hostStorageMap.values()) {
				if (storageNode.taskStore.hasNext()) {
					return true;
				}
			}
			return false;
		}

		public ColorTask next() {
			if (heap.size() == 0) {
				update();
			}
			ColorStorageNode node = heap.poll();
			return node.taskStore.next();
		}

		public void update() {
			this.heap = new PriorityQueue<ColorStorageNode>(100, storageComparator);
			Iterator<ColorStorageNode> iterator = hostStorageMap.values().iterator();
			while (iterator.hasNext()) {
				ColorStorageNode node = iterator.next();
				if (node.taskStore.hasNext()) {
					this.heap.add(node);
				}
			}
		}
	}

	class NodeStore {

		private final Log LOG = LogFactory.getLog(NodeStore.class);

		Comparator<ColorComputingNode> loadingComparator = new Comparator<ColorComputingNode>() {

			@Override
			public int compare(ColorComputingNode arg0, ColorComputingNode arg1) {
				int result = new Double(arg0.loading).compareTo(new Double(arg1.loading));
				if (result == 0) {
					return new Integer(arg0.hashCode()).compareTo(new Integer(arg1.hashCode()));
				}
				return result;
			}
		};

		Comparator<ColorComputingNode> hostComparator = new Comparator<ColorComputingNode>() {

			@Override
			public int compare(ColorComputingNode arg0, ColorComputingNode arg1) {
				return arg0.node.getHostName().compareTo(arg1.node.getHostName());
			}
		};

		PriorityQueue<ColorComputingNode> heap = new PriorityQueue<ColorComputingNode>(100, loadingComparator);
		Set<ColorComputingNode> nodeSet = new HashSet<ColorComputingNode>();

		public NodeStore() {
		}

		public void report() {
			LOG.fatal("Node store: node=" + nodeSet.size());
			for (ColorComputingNode node : nodeSet) {
				LOG.fatal("\tnode=" + node.node.getHostName() + ", loading=" + node.loading + ", availableSlots=" + node.availableSlot);
			}
		}

		public void addNode(FlowSchedulerNode node) {
			ColorComputingNode colorNode = new ColorComputingNode(node);
			nodeSet.add(colorNode);
		}

		public boolean hasNext() {
			if (heap.size() == 0) {
				update();
			}
			return heap.size() > 0;
		}

		public ColorComputingNode next() {
			if (heap.size() == 0) {
				update();
			}
			ColorComputingNode node = heap.poll();
			return node;
		}

		public void update() {
			this.heap = new PriorityQueue<ColorComputingNode>(100, loadingComparator);
			Iterator<ColorComputingNode> iterator = nodeSet.iterator();
			while (iterator.hasNext()) {
				ColorComputingNode node = iterator.next();
				if (node.isAvailable()) {
					this.heap.add(node);
				}
			}
		}
	}

	// --basic----------------

	class ColorTask {
		FlowSchedulerTask task;
		double demand = 0;

		public ColorTask(FlowSchedulerTask task) {
			this.task = task;
			this.demand = task.getFlowDemand();
		}

		@Override
		public String toString() {
			return "ColorTask [task=" + task + ", demand=" + demand + "]";
		}

	}

	class ColorComputingNode {
		FlowSchedulerNode node;
		double loading;
		int availableSlot;

		public ColorComputingNode(FlowSchedulerNode node) {
			this.node = node;
			this.loading = node.getLoad();
			this.availableSlot = node.getAvailableSlots();
		}

		public boolean isAvailable() {
			return this.availableSlot > 0;
		}

		// task is not necessarily worth one slot
		public void assignTask(ColorTask task) {
			this.loading += (task.demand);
			this.availableSlot--;
		}

		@Override
		public String toString() {
			return "ColorComputingNode [node=" + node + ", loading=" + loading + ", availableSlot=" + availableSlot + "]";
		}

	}

	class ColorStorageNode {
		FlowSchedulerStorage node;
		double loading;
		MapTaskStore taskStore = new MapTaskStore();

		public ColorStorageNode(FlowSchedulerStorage node) {
			this.node = node;
			this.loading = node.getLoad();
		}

		public void addTask(FlowSchedulerTask task) {
			taskStore.addTask(task);
		}

		// task is not necessarily worth one slot
		public void assignTask(ColorTask task) {
			this.loading += (task.demand);
		}

		@Override
		public String toString() {
			return "ColorStorageNode [node=" + node + ", loading=" + loading + "]";
		}

	}
}