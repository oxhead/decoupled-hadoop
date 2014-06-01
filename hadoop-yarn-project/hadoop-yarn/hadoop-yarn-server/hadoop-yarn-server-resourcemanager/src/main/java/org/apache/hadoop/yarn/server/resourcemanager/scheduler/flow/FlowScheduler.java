package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.Lock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.util.BuilderUtils;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class FlowScheduler implements ResourceScheduler, Configurable {
	private static final Log LOG = LogFactory.getLog(FlowScheduler.class);

	private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

	Configuration conf;

	private final static Container[] EMPTY_CONTAINER_ARRAY = new Container[] {};
	private final static List<Container> EMPTY_CONTAINER_LIST = Arrays.asList(EMPTY_CONTAINER_ARRAY);
	private RMContext rmContext;

	private boolean initialized;
	private Resource minimumAllocation;
	private Resource maximumAllocation;

	private ActiveUsersManager activeUsersManager;

	private static final String DEFAULT_QUEUE_NAME = "default";
	private QueueMetrics metrics;

	private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

	public int flownetwork_period = 5;
	private ScheduledThreadPoolExecutor updatorPool;
	private FlowSchedulerManager flowSchedulerManager;

	private final Queue DEFAULT_QUEUE = new Queue() {
		@Override
		public String getQueueName() {
			return DEFAULT_QUEUE_NAME;
		}

		@Override
		public QueueMetrics getMetrics() {
			return metrics;
		}

		@Override
		public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
			QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
			queueInfo.setQueueName(DEFAULT_QUEUE.getQueueName());
			queueInfo.setCapacity(1.0f);
			if (clusterResource.getMemory() == 0) {
				queueInfo.setCurrentCapacity(0.0f);
			} else {
				queueInfo.setCurrentCapacity((float) usedResource.getMemory() / clusterResource.getMemory());
			}
			queueInfo.setMaximumCapacity(1.0f);
			queueInfo.setChildQueues(new ArrayList<QueueInfo>());
			queueInfo.setQueueState(QueueState.RUNNING);
			return queueInfo;
		}

		@Override
		public Map<QueueACL, AccessControlList> getQueueAcls() {
			Map<QueueACL, AccessControlList> acls = new HashMap<QueueACL, AccessControlList>();
			for (QueueACL acl : QueueACL.values()) {
				acls.put(acl, new AccessControlList("*"));
			}
			return acls;
		}

		@Override
		public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation unused) {
			QueueUserACLInfo queueUserAclInfo = recordFactory.newRecordInstance(QueueUserACLInfo.class);
			queueUserAclInfo.setQueueName(DEFAULT_QUEUE_NAME);
			queueUserAclInfo.setUserAcls(Arrays.asList(QueueACL.values()));
			return Collections.singletonList(queueUserAclInfo);
		}
	};

	@Override
	public synchronized void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public synchronized Configuration getConf() {
		return conf;
	}

	@Override
	public Resource getMinimumResourceCapability() {
		return minimumAllocation;
	}

	@Override
	public int getNumClusterNodes() {
		return flowSchedulerManager.getNumOfNodes();
	}

	@Override
	public Resource getMaximumResourceCapability() {
		return maximumAllocation;
	}

	public String getAssignmentMode() {
		return ((FlowSchedulerConfiguration) getConf()).getAssignmentModel();
	}

	@Override
	public synchronized void reinitialize(Configuration conf, RMContext rmContext) throws IOException {
		FlowSchedulerConfiguration newConf = new FlowSchedulerConfiguration(conf);
		setConf(newConf);
		if (!this.initialized) {
			this.rmContext = rmContext;
			this.minimumAllocation = Resources.createResource(conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
			this.maximumAllocation = Resources.createResource(conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
			this.metrics = QueueMetrics.forQueue(DEFAULT_QUEUE_NAME, null, false, conf);
			this.activeUsersManager = new ActiveUsersManager(metrics);
			this.initialized = true;

			this.flownetwork_period = newConf.getFlowNetworkPeriod();
			this.updatorPool = new ScheduledThreadPoolExecutor(100);
			this.flowSchedulerManager = new FlowSchedulerManager(rmContext, newConf);

			updatorPool.scheduleAtFixedRate(new Runnable() {

				@Override
				public void run() {
					try {
						// printStatus();
						updateFlowNetwork();
					} catch (Throwable t) {
						LOG.fatal("MinCostFlow module worked abnormally!!!", t);
					}
				}
			}, 10, flownetwork_period, TimeUnit.SECONDS);
		}
	}

	private static final Allocation EMPTY_ALLOCATION = new Allocation(EMPTY_CONTAINER_LIST, Resources.createResource(0));

	@Override
	public Allocation allocate(ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask, List<ContainerId> release) {
		FlowSchedulerApp application = flowSchedulerManager.lookupApp(applicationAttemptId);
		if (application == null) {
			LOG.error("Calling allocate on removed " + "or non existant application " + applicationAttemptId);
			return EMPTY_ALLOCATION;
		}
		for (ResourceRequest rr : ask) {
			LOG.fatal("[Ask] Application (" + application.getApplicationId() + "): " + rr + " -> " + rr.getNumContainers() + " => " + rr.getHostName());
		}
		for (ContainerId id : release) {
			LOG.fatal("[Release] Application (" + application.getApplicationId() + "): container (" + id + ")");
		}

		// Sanity check
		SchedulerUtils.normalizeRequests(ask, resourceCalculator, clusterResource, minimumAllocation);

		// Release containers
		for (ContainerId releasedContainer : release) {
			RMContainer rmContainer = getRMContainer(releasedContainer);
			if (rmContainer == null) {
				RMAuditLogger.logFailure(application.getUser(), AuditConstants.RELEASE_CONTAINER, "Unauthorized access or invalid container", "FifoScheduler",
						"Trying to release container not owned by app or with invalid id", application.getApplicationId(), releasedContainer);
			}
			containerCompleted(rmContainer.getContainerId(), SchedulerUtils.createAbnormalContainerStatus(releasedContainer, SchedulerUtils.RELEASED_CONTAINER), RMContainerEventType.RELEASED);
		}

		synchronized (application) {
			if (!ask.isEmpty()) {
				// Update application requests
				application.updateResourceRequests(ask);
			}

			List<Container> list = application.pullNewlyAllocatedContainers();
			return new Allocation(list, application.getHeadroom());
		}
	}

	@Override
	public SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId applicationAttemptId) {
		FlowSchedulerApp app = flowSchedulerManager.lookupApp(applicationAttemptId);
		return app == null ? null : new SchedulerAppReport(app);
	}

	private synchronized void addApplication(ApplicationAttemptId appAttemptId, String user) {
		// TODO: Fix store
		FlowSchedulerApp schedulerApp = new FlowSchedulerApp(appAttemptId, user, DEFAULT_QUEUE, activeUsersManager, this.rmContext, flowSchedulerManager);
		flowSchedulerManager.addApp(schedulerApp);

		metrics.submitApp(user, appAttemptId.getAttemptId());
		LOG.info("Application Submission: " + appAttemptId.getApplicationId() + " from " + user + ", currently active: " + flowSchedulerManager.getNumOfApps());
		rmContext.getDispatcher().getEventHandler().handle(new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.APP_ACCEPTED));

	}

	private synchronized void doneApplication(ApplicationAttemptId applicationAttemptId, RMAppAttemptState rmAppAttemptFinalState) throws IOException {
		FlowSchedulerApp application = flowSchedulerManager.lookupApp(applicationAttemptId);
		if (application == null) {
			throw new IOException("Unknown application " + applicationAttemptId + " has completed!");
		}

		// Kill all 'live' containers
		for (RMContainer container : application.getLiveContainers()) {
			containerCompleted(container.getContainerId(), SchedulerUtils.createAbnormalContainerStatus(container.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION), RMContainerEventType.KILL);
		}

		// Inform the activeUsersManager
		synchronized (application) {
			activeUsersManager.deactivateApplication(application.getUser(), application.getApplicationId());
		}

		// Clean up pending requests, metrics etc.
		application.stop(rmAppAttemptFinalState);

		// Remove the application
		flowSchedulerManager.removeApp(applicationAttemptId);
	}

	private boolean assignContainer(FlowSchedulerApp application, FlowSchedulerTask task, FlowSchedulerNode node) {
		if (!node.isAvailable(task.getResourceDemand())) {
			LOG.fatal("Unable to assign task " + task + " to node " + node);
			return false;
		}

		NodeId nodeId = node.getRMNode().getNodeID();
		ContainerId containerId = BuilderUtils.newContainerId(application.getApplicationAttemptId(), application.getNewContainerId());
		ContainerToken containerToken = null;

		// If security is enabled, send the container-tokens too.
		if (UserGroupInformation.isSecurityEnabled()) {
			containerToken = this.rmContext.getContainerTokenSecretManager().createContainerToken(containerId, nodeId, application.getUser(), task.getResourceDemand());
			if (containerToken == null) {
				return false;
			}
		}

		// TODO add allocation hint...
		// Create the container
		Container container = BuilderUtils.newContainer(containerId, nodeId, node.getRMNode().getHttpAddress(), task.getResourceDemand(), task.getPriority(), containerToken);
		if (task.getAttemptId() != null) {
			container.setAllocationHint(task.getAttemptId());
			LOG.fatal("[Hint] container=" + container.getId() + ", hint=" + container.getAllocationHint());
		} else {
			LOG.fatal("[Hint] container=" + container.getId() + " -> no hint");
		}

		// Allocate!
		// Inform the application
		NodeType type = NodeType.OFF_SWITCH;
		Priority priority = task.getPriority();
		LOG.fatal("@ task=" + task.getAttemptId() + ", priority=" + priority);
		ResourceRequest request = application.getResourceRequest(priority, FlowSchedulerNode.ANY);
		RMContainer rmContainer = application.allocate(type, node, priority, request, container);
		if (rmContainer != null) {
			// Inform the node
			flowSchedulerManager.launchTask(task, rmContainer);
			LOG.fatal("$ assign:" + rmContainer.getContainerId() + " on " + node.getHostName() + " to " + application.getApplicationAttemptId());

			// Update usage for this container
			Resources.addTo(usedResource, task.getResourceDemand());
		} else {
			LOG.fatal("$ allication cannot allocate container");

		}
		return true;

	}

	private synchronized void nodeUpdate(RMNode rmNode, List<ContainerStatus> newlyLaunchedContainers, List<ContainerStatus> completedContainers) {
		FlowSchedulerNode node = flowSchedulerManager.lookupNode(rmNode.getNodeID());

		for (ContainerStatus cs : newlyLaunchedContainers) {
			LOG.fatal("Node update launched container: " + node.getHostName() + " -> " + cs.getContainerId());
		}
		for (ContainerStatus cs : completedContainers) {
			LOG.fatal("Node update completed container: " + node.getHostName() + " -> " + cs.getContainerId());
		}

		// Processing the newly launched containers
		for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
			containerLaunchedOnNode(launchedContainer.getContainerId(), rmNode.getNodeID());
		}

		// Process completed containers
		for (ContainerStatus completedContainer : completedContainers) {
			ContainerId containerId = completedContainer.getContainerId();
			RMContainer container = getRMContainer(containerId);
			if (container == null) {
				LOG.fatal("Skip null container: " + containerId);
				continue;
			}
			containerCompleted(containerId, completedContainer, RMContainerEventType.FINISHED);
		}

		// Disable assignment on node udpate
		if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource, node.getAvailableResource(), minimumAllocation)) {
			// updateFlowNetwork();
			// assignContainers(node);
		}

		metrics.setAvailableResourcesToQueue(Resources.subtract(clusterResource, usedResource));

	}

	@Override
	public void handle(SchedulerEvent event) {
		switch (event.getType()) {
		case NODE_ADDED: {
			NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
			addNode(nodeAddedEvent.getAddedRMNode());
		}
			break;
		case NODE_REMOVED: {
			NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent) event;
			removeNode(nodeRemovedEvent.getRemovedRMNode());
		}
			break;
		case NODE_UPDATE: {
			NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent) event;
			nodeUpdate(nodeUpdatedEvent.getRMNode(), nodeUpdatedEvent.getNewlyLaunchedContainers(), nodeUpdatedEvent.getCompletedContainers());
		}
			break;
		case APP_ADDED: {
			AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
			addApplication(appAddedEvent.getApplicationAttemptId(), appAddedEvent.getUser());
		}
			break;
		case APP_REMOVED: {
			AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent) event;
			try {
				doneApplication(appRemovedEvent.getApplicationAttemptID(), appRemovedEvent.getFinalAttemptState());
			} catch (IOException ie) {
				LOG.error("Unable to remove application " + appRemovedEvent.getApplicationAttemptID(), ie);
			}
		}
			break;
		case CONTAINER_EXPIRED: {
			ContainerExpiredSchedulerEvent containerExpiredEvent = (ContainerExpiredSchedulerEvent) event;
			ContainerId containerid = containerExpiredEvent.getContainerId();
			containerCompleted(containerid, SchedulerUtils.createAbnormalContainerStatus(containerid, SchedulerUtils.EXPIRED_CONTAINER), RMContainerEventType.EXPIRE);
		}
			break;
		default:
			LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
		}
	}

	private void containerLaunchedOnNode(ContainerId containerId, NodeId nodeId) {
		// Get the application for the finished container
		ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
		FlowSchedulerApp application = flowSchedulerManager.lookupApp(applicationAttemptId);
		if (application == null) {
			LOG.info("Unknown application: " + applicationAttemptId + " launched container " + containerId + " on node: " + nodeId);
			// Some unknown container sneaked into the system. Kill it.
			this.rmContext.getDispatcher().getEventHandler().handle(new RMNodeCleanContainerEvent(nodeId, containerId));

			return;
		}

		flowSchedulerManager.launchedContainer(containerId, nodeId);
	}

	@Lock(FlowScheduler.class)
	private synchronized void containerCompleted(ContainerId containerId, ContainerStatus containerStatus, RMContainerEventType event) {
		FlowSchedulerTask task = flowSchedulerManager.lookupTask(containerId);
		flowSchedulerManager.completeTask(task, containerStatus, event);

		// Update total usage
		Resources.subtractFrom(usedResource, task.getRmContainer().getContainer().getResource());
	}

	private Resource clusterResource = recordFactory.newRecordInstance(Resource.class);
	private Resource usedResource = recordFactory.newRecordInstance(Resource.class);

	@Override
	public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues, boolean recursive) {
		return DEFAULT_QUEUE.getQueueInfo(false, false);
	}

	@Override
	public List<QueueUserACLInfo> getQueueUserAclInfo() {
		return DEFAULT_QUEUE.getQueueUserAclInfo(null);
	}

	public List<FlowSchedulerNode> getNodes() {
		return flowSchedulerManager.getNodes();
	}

	private synchronized void addNode(RMNode nodeManager) {
		double capacity = ((FlowSchedulerConfiguration) getConf()).getNodeCapacity(nodeManager.getHostName());
		FlowSchedulerNode node = new FlowSchedulerNode(nodeManager, capacity, this.minimumAllocation.getMemory());
		flowSchedulerManager.addNode(node);
		Resources.addTo(clusterResource, nodeManager.getTotalCapability());
	}

	private synchronized void removeNode(RMNode nodeInfo) {
		FlowSchedulerNode node = flowSchedulerManager.lookupNode(nodeInfo.getNodeID());
		// Kill running containers
		for (RMContainer container : node.getRunningContainers()) {
			containerCompleted(container.getContainerId(), SchedulerUtils.createAbnormalContainerStatus(container.getContainerId(), SchedulerUtils.LOST_CONTAINER), RMContainerEventType.KILL);
		}

		flowSchedulerManager.removeNode(nodeInfo.getNodeID());

		// Update cluster metrics
		Resources.subtractFrom(clusterResource, node.getRMNode().getTotalCapability());
	}

	@Override
	public void recover(RMState state) {
		// NOT IMPLEMENTED
	}

	@Override
	public synchronized SchedulerNodeReport getNodeReport(NodeId nodeId) {
		FlowSchedulerNode node = flowSchedulerManager.lookupNode(nodeId);
		return node == null ? null : new SchedulerNodeReport(node);
	}

	private RMContainer getRMContainer(ContainerId containerId) {
		FlowSchedulerApp application = flowSchedulerManager.lookupApp(containerId.getApplicationAttemptId());
		return (application == null) ? null : application.getRMContainer(containerId);
	}

	@Override
	public QueueMetrics getRootQueueMetrics() {
		return DEFAULT_QUEUE.getMetrics();
	}

	private void printTasks(List<FlowSchedulerTask> tasks) {
		LOG.fatal("Unscheduled tasks: " + tasks.size());
		for (FlowSchedulerTask task : tasks) {
			LOG.fatal("\ttask=" + task.getAttemptId() + ", type=" + task.getType() + ", job=" + task.getApp().getApplicationName());
		}
	}

	private synchronized void updateFlowNetwork() {
		// LOG.fatal("#################################################");
		// LOG.fatal("# Update flow network: " + applications.size());
		// LOG.fatal("# cluster: " + clusterResource + ", used: " +
		// usedResource);
		// LOG.fatal("#################################################");

		List<FlowSchedulerNode> availableNodes = flowSchedulerManager.getAvailableNodes();
		List<FlowSchedulerTask> unscheduledTasks = flowSchedulerManager.getUnscheduledTasks();
		List<FlowSchedulerStorage> dataStorages = flowSchedulerManager.getStorages();

		if (availableNodes.size() == 0 || unscheduledTasks.size() == 0) {
			LOG.fatal("[Scheduler] no need to schedule");
			return;
		}

		printTasks(unscheduledTasks);

		// Ready to assign tasks onto nodes
		List<Assignment> assignments = null;

		if (unscheduledTasks.size() == 0) {
			LOG.fatal("[Scheduler] no more tasks to scheduel");
			return;
		} else {
			LOG.fatal("[Scheduler] unscheduled tasks: " + unscheduledTasks.size());
		}

		String modelName = ((FlowSchedulerConfiguration) getConf()).getAssignmentModel();
		AssignmentModel model = null;
		if (modelName.equalsIgnoreCase("Balancing")) {
			// model = new BalancingModel(nodes, tasks);
			LOG.fatal("[Assignment] model: Balancing Model");
		} else if (modelName.equalsIgnoreCase("Fifo")) {
			// model = new FifoModel(nodes, tasks);
			LOG.fatal("[Assignment] model: Fifo Model");
		} else if (modelName.equalsIgnoreCase("Bulk")) {
			// model = new BulkModel(nodes, tasks);
			LOG.fatal("[Assignment] model: Bulk Model");
		} else if (modelName.equalsIgnoreCase("Color")) {
			// model = new ColorModel(nodes, tasks);
			LOG.fatal("[Assignment] model: Color Model");
		} else if (modelName.equalsIgnoreCase("ColorStorage")) {
			model = new ColorStorageModel(availableNodes, unscheduledTasks, dataStorages);
			LOG.fatal("[Assignment] model: Color Storag Model");
		} else {
			model = new FlowModel(availableNodes, dataStorages, unscheduledTasks, (FlowSchedulerConfiguration) getConf());
			LOG.fatal("[Assignment] model: Min Cost Flow Model");
		}

		LOG.fatal("[Assignment] model: " + model.getClass());

		model.construct();
		model.solve();
		assignments = model.constructAssignments();

		LOG.fatal("[Scheduler] # of assignments: " + assignments.size());

		printAssignmentResults(assignments);

		// Allocate containers to tasks
		// TODO: retrieve assignment result, assigning...
		for (Assignment assignment : assignments) {
			FlowSchedulerNode node = assignment.getNode();
			FlowSchedulerTask task = assignment.getTask();
			FlowSchedulerApp application = task.getApp();
			Priority priority = task.getPriority();
			ResourceRequest request = application.getResourceRequest(priority, FlowSchedulerNode.ANY);
			// tasks can be launched during scheduling period and this case can
			// happen
			if (request != null) {
				assignContainer(application, task, node);
			}
			if (task.getStorage() != null) {
				LOG.fatal("[Storage] " + task.getStorage().getHost() + " <-" + task.getAttemptId());
			}
		}

		// Update cluster resource usage in application's view
		for (FlowSchedulerApp application : flowSchedulerManager.getApps()) {
			application.setHeadroom(Resources.subtract(clusterResource, usedResource));
		}

		metrics.setAvailableResourcesToQueue(Resources.subtract(clusterResource, usedResource));

	}

	private void printAssignmentResults(List<Assignment> assignments) {
		Map<FlowSchedulerNode, List<FlowSchedulerTask>> assignmentMap = new HashMap<FlowSchedulerNode, List<FlowSchedulerTask>>();
		for (FlowSchedulerNode node : flowSchedulerManager.getNodes()) {
			assignmentMap.put(node, new LinkedList<FlowSchedulerTask>());
		}
		for (Assignment assignment : assignments) {
			List<FlowSchedulerTask> tasks = assignmentMap.get(assignment.node);
			tasks.add(assignment.task);
			assignmentMap.put(assignment.node, tasks);
		}

		for (FlowSchedulerNode node : flowSchedulerManager.getNodes()) {
			LOG.fatal("[] " + node);
			for (FlowSchedulerTask task : assignmentMap.get(node)) {
				LOG.fatal("\t<new> " + task);
			}
		}
	}

}
