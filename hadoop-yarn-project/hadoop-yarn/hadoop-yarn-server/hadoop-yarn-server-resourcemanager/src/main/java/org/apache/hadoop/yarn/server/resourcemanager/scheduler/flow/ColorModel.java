package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkTask;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.NetworkFlowSolver.Solution;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.Task.Type;

public class ColorModel implements AssignmentModel {

	private static final Log LOG = LogFactory.getLog(ColorModel.class);

	List<FlowNetworkNode> nodes;
	List<FlowNetworkTask> tasks;
	List<Assignment> assignments = new LinkedList<Assignment>();

	TaskStore taskStore;
	NodeStore nodeStore;

	public ColorModel(List<FlowNetworkNode> nodes, List<FlowNetworkTask> tasks) {
		this.nodes = nodes;
		this.tasks = tasks;
	}

	public void construct() {
		LOG.fatal("^^ add tasks: " + tasks.size());
		taskStore = new TaskStore(tasks);
		nodeStore = new NodeStore(nodes);
	}

	// TODO: here might be wrong, should assign from larger slot?
	public List<Solution> solve() {

		assignments.clear();

		// evenly distribution should be enough
		while (nodeStore.hasNext() && taskStore.hasMoreAppMaster()) {
			ColorNode node = nodeStore.next();
			ColorTask task = taskStore.nextkAppMaster();
			node.assignTask(task);
			Assignment assignment = new Assignment(task.task, node.node);
			assignments.add(assignment);
			node.assignTask(task);
			LOG.fatal("@@ assign " + task + " to " + node);
		}

		while (nodeStore.hasNext() && taskStore.hasMoreReduce()) {
			ColorNode node = nodeStore.next();
			ColorTask task = taskStore.nextReduce();
			if (!node.willBeOverloaded(task)) {
				node.assignTask(task);
				Assignment assignment = new Assignment(task.task, node.node);
				assignments.add(assignment);
				LOG.fatal("@@ assign " + task + " to " + node);
			} else {
				if (node.isMostFree()) {
					node.assignTask(task);
					Assignment assignment = new Assignment(task.task, node.node);
					assignments.add(assignment);
					LOG.fatal("@@ assign " + task + " to " + node);
				} else {
					LOG.fatal("@@ Unable to assign " + task);
				}
			}
		}

		// TODO: why too many tasks are added
		while (nodeStore.hasNext() && taskStore.hasMoreMap()) {
			ColorNode node = nodeStore.next();
			ColorTask task = taskStore.nextMap();
			if (!node.willBeOverloaded(task)) {
				Assignment assignment = new Assignment(task.task, node.node);
				assignments.add(assignment);
				node.assignTask(task);
				LOG.fatal("@@ assign " + task + " to " + node);
			} else {
				LOG.fatal("@@ Unable to assign " + task);
			}
		}

		return new ArrayList<NetworkFlowSolver.Solution>();
	}

	@Override
	public List<Assignment> constructAssignments(List<Solution> solutions) {
		return assignments;
	}
}

class TaskStore {
	// The map tasks in the queue will be ordered by job id
	Comparator<ColorTask> mapTaskComparator = new Comparator<ColorTask>() {

		@Override
		public int compare(ColorTask o1, ColorTask o2) {
			int result = o1.compareTo(o2);
			if (result == 0) {
				return o1.task.app.app.getApplicationId().compareTo(o2.task.app.app.getApplicationId());
			}
			return result;
		}
	};

	Comparator<ColorTask> reduceTaskComparator = new Comparator<ColorTask>() {

		@Override
		public int compare(ColorTask o1, ColorTask o2) {
			int result = o1.compareTo(o2);
			if (result == 0) {
				return o1.task.app.app.getApplicationId().compareTo(o2.task.app.app.getApplicationId());
			}
			return -result;
		}
	};

	Comparator<ColorTask> appMasterTaskComparator = new Comparator<ColorTask>() {

		@Override
		public int compare(ColorTask o1, ColorTask o2) {
			return o1.task.app.app.getApplicationId().compareTo(o2.task.app.app.getApplicationId());
		}
	};

	private static final Log LOG = LogFactory.getLog(TaskStore.class);
	Map<Double, TreeSet<ColorTask>> store = new HashMap<Double, TreeSet<ColorTask>>();

	TreeSet<ColorTask> reduceQueue = new TreeSet<ColorTask>(reduceTaskComparator);
	TreeSet<ColorTask> appMasterQueue = new TreeSet<ColorTask>(appMasterTaskComparator);

	PriorityQueue<Double> currentHeap = new PriorityQueue<Double>();
	Double currentKey;

	public TaskStore(List<FlowNetworkTask> tasks) {
		LOG.fatal("&& add tasks: " + tasks.size());
		this.addTasks(tasks);
		update();
	}

	public void addTasks(Iterable<FlowNetworkTask> tasks) {
		for (FlowNetworkTask task : tasks) {
			addTask(task);
		}
	}

	public void addTask(FlowNetworkTask task) {
		ColorTask colorTask = new ColorTask(task);
		if (task.type.equals(Type.AppMaster)) {
			appMasterQueue.add(colorTask);
		} else if (task.type.equals(Type.Reduce)) {
			reduceQueue.add(colorTask);
		} else {
			if (!store.containsKey(colorTask.demand)) {
				store.put(colorTask.demand, new TreeSet<ColorTask>(mapTaskComparator));
			}
			TreeSet<ColorTask> set = store.get(colorTask.demand);
			set.add(colorTask);
		}

	}

	public void update() {
		LOG.fatal("** before update -> heap size=" + currentHeap.size());
		PriorityQueue<Double> heap = new PriorityQueue<Double>();
		Iterator<Double> keyIterator = store.keySet().iterator();
		List<Double> removedKey = new LinkedList<Double>();
		while (keyIterator.hasNext()) {
			Double key = keyIterator.next();
			LOG.fatal("++ task queue=" + store.get(key).size());
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
		LOG.fatal("** after update -> heap size=" + currentHeap.size());
	}

	public boolean hasMoreMap() {
		if (currentHeap.size() == 0) {
			update();
		}
		return currentHeap.size() > 0;
	}

	public ColorTask nextMap() {
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

	public boolean hasMoreAppMaster() {
		return appMasterQueue.size() > 0;
	}

	public ColorTask nextkAppMaster() {
		return appMasterQueue.pollFirst();
	}

	public boolean hasMoreReduce() {
		return reduceQueue.size() > 0;
	}

	public ColorTask nextReduce() {
		return reduceQueue.pollFirst();
	}
}

class NodeStore {

	PriorityQueue<ColorNode> heap = new PriorityQueue<ColorNode>();
	Set<ColorNode> nodeSet = new HashSet<ColorNode>();

	public NodeStore(List<FlowNetworkNode> nodes) {
		for (FlowNetworkNode node : nodes) {
			ColorNode colorNode = new ColorNode(node);
			nodeSet.add(colorNode);
		}
		update();
	}

	public boolean hasNext() {
		if (heap.size() == 0) {
			update();
		}
		return heap.size() > 0;
	}

	public ColorNode next() {
		if (heap.size() == 0) {
			update();
		}
		ColorNode node = heap.poll();
		return node;
	}

	public void update() {
		this.heap = new PriorityQueue<ColorNode>();
		Iterator<ColorNode> iterator = nodeSet.iterator();
		while (iterator.hasNext()) {
			ColorNode node = iterator.next();
			if (node.isAvailable()) {
				this.heap.add(node);
			}
		}
	}
}

class ColorTask implements Comparable<ColorTask> {
	FlowNetworkTask task;
	double demand = 0;

	public ColorTask(FlowNetworkTask task) {
		this.task = task;
		if (task.type.equals(Type.Map)) {
			this.demand = task.flowRate.flowIn + task.flowRate.flowOut;
		} else if (task.type.equals(Type.Reduce)) {
			this.demand = (task.app.getNumOfMapTasks() / task.app.getNumOfReduceTasks()) * (task.flowRate.flowIn + task.flowRate.flowOut);
		}
	}

	@Override
	public int compareTo(ColorTask o) {
		return new Double(this.demand).compareTo(new Double(o.demand));
	}

	@Override
	public String toString() {
		return "ColorTask [task=" + task + ", demand=" + demand + "]";
	}

}

class ColorNode implements Comparable<ColorNode> {
	FlowNetworkNode node;
	double loading;
	int availableSlot;

	public ColorNode(FlowNetworkNode node) {
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

	public boolean willBeOverloaded(ColorTask task) {
		if ((this.loading + task.demand) > this.node.capacity * 4) {
			return true;
		}
		return false;
	}

	public boolean isMostFree() {
		return this.availableSlot > 4;
	}

	@Override
	public int compareTo(ColorNode arg0) {
		return new Double(this.loading).compareTo(new Double(arg0.loading));
	}

	@Override
	public String toString() {
		return "ColorNode [node=" + node + ", loading=" + loading + ", availableSlot=" + availableSlot + "]";
	}

}
