package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkStorage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowNetworkMonitor.FlowNetworkTask;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.NetworkFlowSolver.Solution;
import org.apache.hadoop.yarn.util.BuilderUtils;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class FlowScheduler extends FifoScheduler implements ResourceScheduler, Configurable {
	private static final Log LOG = LogFactory.getLog(FlowScheduler.class);

	private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

	Configuration conf;

	private final static Container[] EMPTY_CONTAINER_ARRAY = new Container[] {};
	private final static List<Container> EMPTY_CONTAINER_LIST = Arrays.asList(EMPTY_CONTAINER_ARRAY);
	private RMContext rmContext;

	private Map<NodeId, FiCaSchedulerNode> nodes = new ConcurrentHashMap<NodeId, FiCaSchedulerNode>();

	private boolean initialized;
	private Resource minimumAllocation;
	private Resource maximumAllocation;

	private Map<ApplicationAttemptId, FiCaSchedulerApp> applications = new TreeMap<ApplicationAttemptId, FiCaSchedulerApp>();

	private ActiveUsersManager activeUsersManager;

	private static final String DEFAULT_QUEUE_NAME = "default";
	private QueueMetrics metrics;

	private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

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
		return nodes.size();
	}

	@Override
	public Resource getMaximumResourceCapability() {
		return maximumAllocation;
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
			this.flowNetworkMonitor = new FlowNetworkMonitor(rmContext, newConf);
			solver = new CS2Solver(newConf.getSolverPath());

			updatorPool.scheduleAtFixedRate(new Runnable() {

				@Override
				public void run() {
					try {
						printStatus();
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
		FiCaSchedulerApp application = getApplication(applicationAttemptId);
		if (application == null) {
			LOG.error("Calling allocate on removed " + "or non existant application " + applicationAttemptId);
			return EMPTY_ALLOCATION;
		}
		// LOG.fatal("Allocate resources: " + application.getApplicationId() +
		// " -> ");
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
			containerCompleted(rmContainer, SchedulerUtils.createAbnormalContainerStatus(releasedContainer, SchedulerUtils.RELEASED_CONTAINER), RMContainerEventType.RELEASED);
		}

		synchronized (application) {
			if (!ask.isEmpty()) {
				// LOG.fatal("allocate:" + " applicationId=" +
				// applicationAttemptId + " #ask=" + ask.size());
				// LOG.fatal("allocate: pre-update" + " applicationId=" +
				// applicationAttemptId + " application=" +
				// application.getApplicationId());

				// application.showRequests();
				// LOG.fatal("Before updating ask requests");
				// showRequests(application);

				// Update application requests
				application.updateResourceRequests(ask);

				// LOG.fatal("allocate: post-update" + " applicationId=" +
				// applicationAttemptId + " application=" +
				// application.getApplicationId());
				// application.showRequests();
				// LOG.fatal("After updating ask requests");
				// showRequests(application);
			}

			List<Container> list = application.pullNewlyAllocatedContainers();
			if (list.size() > 0) {
				for (Container c : list) {
					LOG.fatal("[Newlly] Application (" + application.getApplicationId() + "): container (" + c.getId() + ") -> " + c.getNodeId() + ": " + c.getNodeHttpAddress());
				}
			}
			return new Allocation(list, application.getHeadroom());
		}
	}

	private FiCaSchedulerApp getApplication(ApplicationAttemptId applicationAttemptId) {
		return applications.get(applicationAttemptId);
	}

	@Override
	public SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId applicationAttemptId) {
		FiCaSchedulerApp app = getApplication(applicationAttemptId);
		return app == null ? null : new SchedulerAppReport(app);
	}

	private FiCaSchedulerNode getNode(NodeId nodeId) {
		return nodes.get(nodeId);
	}

	private synchronized void addApplication(ApplicationAttemptId appAttemptId, String user) {
		// TODO: Fix store
		FiCaSchedulerApp schedulerApp = new FiCaSchedulerApp(appAttemptId, user, DEFAULT_QUEUE, activeUsersManager, this.rmContext);
		applications.put(appAttemptId, schedulerApp);
		metrics.submitApp(user, appAttemptId.getAttemptId());
		LOG.info("Application Submission: " + appAttemptId.getApplicationId() + " from " + user + ", currently active: " + applications.size());
		rmContext.getDispatcher().getEventHandler().handle(new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.APP_ACCEPTED));

		flowNetworkMonitor.addApp(schedulerApp);
	}

	private synchronized void doneApplication(ApplicationAttemptId applicationAttemptId, RMAppAttemptState rmAppAttemptFinalState) throws IOException {
		FiCaSchedulerApp application = getApplication(applicationAttemptId);
		if (application == null) {
			throw new IOException("Unknown application " + applicationAttemptId + " has completed!");
		}

		// Kill all 'live' containers
		for (RMContainer container : application.getLiveContainers()) {
			containerCompleted(container, SchedulerUtils.createAbnormalContainerStatus(container.getContainerId(), SchedulerUtils.COMPLETED_APPLICATION), RMContainerEventType.KILL);
		}

		// Inform the activeUsersManager
		synchronized (application) {
			activeUsersManager.deactivateApplication(application.getUser(), application.getApplicationId());
		}

		// Clean up pending requests, metrics etc.
		application.stop(rmAppAttemptFinalState);

		// Remove the application
		applications.remove(applicationAttemptId);

		flowNetworkMonitor.removeApp(application);
	}

	/**
	 * Heart of the scheduler...
	 * 
	 * @param node
	 *            node on which resources are available to be allocated
	 */
	private void assignContainers(FiCaSchedulerNode node) {
		LOG.debug("assignContainers:" + " node=" + node.getRMNode().getNodeAddress() + " #applications=" + applications.size());

		// List<FiCaSchedulerApp> appList = new
		// ArrayList<FiCaSchedulerApp>(applications.values());
		// shuffleList(appList);
		// Try to assign containers to applications in fifo order
		for (Map.Entry<ApplicationAttemptId, FiCaSchedulerApp> e : applications.entrySet()) {
			// for (FiCaSchedulerApp application : appList) {
			FiCaSchedulerApp application = e.getValue();
			LOG.debug("pre-assignContainers");
			application.showRequests();
			synchronized (application) {
				for (Priority priority : application.getPriorities()) {
					int maxContainers = getMaxAllocatableContainers(application, priority, node, NodeType.OFF_SWITCH);
					// Ensure the application needs containers of this priority
					if (maxContainers > 0) {
						int assignedContainers = assignContainersOnNode(node, application, priority);
						// Do not assign out of order w.r.t priorities
						if (assignedContainers == 0) {
							break;
						}
					}
				}
			}

			LOG.debug("post-assignContainers");
			application.showRequests();

			// Done
			if (Resources.lessThan(resourceCalculator, clusterResource, node.getAvailableResource(), minimumAllocation)) {
				break;
			}
		}

		// Update the applications' headroom to correctly take into
		// account the containers assigned in this update.
		for (FiCaSchedulerApp application : applications.values()) {
			application.setHeadroom(Resources.subtract(clusterResource, usedResource));
		}
	}

	private int getMaxAllocatableContainers(FiCaSchedulerApp application, Priority priority, FiCaSchedulerNode node, NodeType type) {
		ResourceRequest offSwitchRequest = application.getResourceRequest(priority, FiCaSchedulerNode.ANY);
		int maxContainers = offSwitchRequest.getNumContainers();

		if (type == NodeType.OFF_SWITCH) {
			return maxContainers;
		}

		if (type == NodeType.RACK_LOCAL) {
			ResourceRequest rackLocalRequest = application.getResourceRequest(priority, node.getRMNode().getRackName());
			if (rackLocalRequest == null) {
				return maxContainers;
			}

			maxContainers = Math.min(maxContainers, rackLocalRequest.getNumContainers());
		}

		if (type == NodeType.NODE_LOCAL) {
			ResourceRequest nodeLocalRequest = application.getResourceRequest(priority, node.getRMNode().getNodeAddress());
			if (nodeLocalRequest != null) {
				maxContainers = Math.min(maxContainers, nodeLocalRequest.getNumContainers());
			}
		}

		return maxContainers;
	}

	private int assignContainersOnNode(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority) {
		// Data-local
		int nodeLocalContainers = assignNodeLocalContainers(node, application, priority);

		// Rack-local
		int rackLocalContainers = assignRackLocalContainers(node, application, priority);

		// Off-switch
		int offSwitchContainers = assignOffSwitchContainers(node, application, priority);

		LOG.debug("assignContainersOnNode:" + " node=" + node.getRMNode().getNodeAddress() + " application=" + application.getApplicationId().getId() + " priority=" + priority.getPriority()
				+ " #assigned=" + (nodeLocalContainers + rackLocalContainers + offSwitchContainers));

		return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
	}

	private int assignNodeLocalContainers(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority) {
		int assignedContainers = 0;
		ResourceRequest request = application.getResourceRequest(priority, node.getRMNode().getNodeAddress());
		LOG.fatal("? assign local node: " + node.getRMNode().getNodeAddress() + " -> " + request);
		if (request != null) {
			// Don't allocate on this node if we don't need containers on this
			// rack
			ResourceRequest rackRequest = application.getResourceRequest(priority, node.getRMNode().getRackName());
			if (rackRequest == null || rackRequest.getNumContainers() <= 0) {
				return 0;
			}
			int assignableContainers = Math.min(getMaxAllocatableContainers(application, priority, node, NodeType.NODE_LOCAL), request.getNumContainers());
			assignedContainers = assignContainer(node, application, priority, assignableContainers, request, NodeType.NODE_LOCAL);
		}
		return assignedContainers;
	}

	private int assignRackLocalContainers(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority) {
		int assignedContainers = 0;
		ResourceRequest request = application.getResourceRequest(priority, node.getRMNode().getRackName());
		LOG.fatal("? assign local rack: " + node.getRMNode().getRackName() + " -> " + request);
		if (request != null) {
			// Don't allocate on this rack if the application doens't need
			// containers
			ResourceRequest offSwitchRequest = application.getResourceRequest(priority, FiCaSchedulerNode.ANY);
			if (offSwitchRequest.getNumContainers() <= 0) {
				return 0;
			}

			int assignableContainers = Math.min(getMaxAllocatableContainers(application, priority, node, NodeType.RACK_LOCAL), request.getNumContainers());
			assignedContainers = assignContainer(node, application, priority, assignableContainers, request, NodeType.RACK_LOCAL);
		}
		return assignedContainers;
	}

	private int assignOffSwitchContainers(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority) {
		int assignedContainers = 0;
		ResourceRequest request = application.getResourceRequest(priority, FiCaSchedulerNode.ANY);
		LOG.fatal("? assign any node: " + FiCaSchedulerNode.ANY + " -> " + request);
		if (request != null) {
			assignedContainers = assignContainer(node, application, priority, request.getNumContainers(), request, NodeType.OFF_SWITCH);
		}
		return assignedContainers;
	}

	private int assignContainer(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority, int assignableContainers, ResourceRequest request, NodeType type) {
		return assignContainer(node, application, priority, assignableContainers, request, type, null);
	}

	private int assignContainer(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority, int assignableContainers, ResourceRequest request, NodeType type, FlowNetworkTask task) {
		LOG.fatal("assignContainer:" + " node=" + node.getRMNode().getNodeAddress() + " application=" + application.getApplicationAttemptId() + " priority=" + priority.getPriority()
				+ " assignableContainers=" + assignableContainers + " request=" + request + " type=" + type);
		Resource capability = request.getCapability();

		// TODO: A buggy
		int availableContainers = node.getAvailableResource().getMemory() / capability.getMemory();
		int assignedContainers = Math.min(assignableContainers, availableContainers);

		LOG.fatal("@ available: " + availableContainers + ", assigned: " + assignableContainers);

		if (assignedContainers > 0) {
			for (int i = 0; i < assignedContainers; ++i) {

				NodeId nodeId = node.getRMNode().getNodeID();
				ContainerId containerId = BuilderUtils.newContainerId(application.getApplicationAttemptId(), application.getNewContainerId());
				ContainerToken containerToken = null;

				// If security is enabled, send the container-tokens too.
				if (UserGroupInformation.isSecurityEnabled()) {
					containerToken = this.rmContext.getContainerTokenSecretManager().createContainerToken(containerId, nodeId, application.getUser(), capability);
					if (containerToken == null) {
						return i; // Try again later.
					}
				}

				// Create the container
				Container container = BuilderUtils.newContainer(containerId, nodeId, node.getRMNode().getHttpAddress(), capability, priority, containerToken);

				// Allocate!
				// Inform the application
				RMContainer rmContainer = application.allocate(type, node, priority, request, container);
				if (rmContainer != null) {
					// Inform the node
					task.rmContainer = rmContainer;
					node.allocateContainer(application.getApplicationId(), rmContainer);
					LOG.fatal("$ assign:" + rmContainer.getContainerId() + " on " + node.getHostName() + " to " + application.getApplicationAttemptId());

					// Update usage for this container
					Resources.addTo(usedResource, capability);
				} else {
					LOG.fatal("$ allication cannot allocate container");
				}

			}

		}

		return assignedContainers;
	}

	private synchronized void nodeUpdate(RMNode rmNode, List<ContainerStatus> newlyLaunchedContainers, List<ContainerStatus> completedContainers) {
		FiCaSchedulerNode node = getNode(rmNode.getNodeID());

		for (ContainerStatus cs : newlyLaunchedContainers) {
			LOG.fatal("Node update launched container: " + node.getHostName() + " -> " + cs.getContainerId());
		}
		for (ContainerStatus cs : completedContainers) {
			LOG.fatal("Node update completed container: " + node.getHostName() + " -> " + cs.getContainerId());
		}

		// Processing the newly launched containers
		for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
			containerLaunchedOnNode(launchedContainer.getContainerId(), node);

			ContainerId containerId = launchedContainer.getContainerId();
			RMContainer container = getRMContainer(containerId);
			FiCaSchedulerApp app = getApplication(container.getApplicationAttemptId());

			flowNetworkMonitor.launchTask(node, app, container);
		}

		// Process completed containers
		for (ContainerStatus completedContainer : completedContainers) {
			ContainerId containerId = completedContainer.getContainerId();
			RMContainer container = getRMContainer(containerId);
			if (container == null) {
				LOG.fatal("Skip null container: " + containerId);
				continue;
			}
			FiCaSchedulerApp app = getApplication(container.getApplicationAttemptId());

			LOG.fatal("Container FINISHED: " + containerId + " -> status: " + completedContainer.getExitStatus() + " => msg: " + completedContainer.getDiagnostics());
			containerCompleted(container, completedContainer, RMContainerEventType.FINISHED);

			flowNetworkMonitor.completeTask(node, container, app);
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
			containerCompleted(getRMContainer(containerid), SchedulerUtils.createAbnormalContainerStatus(containerid, SchedulerUtils.EXPIRED_CONTAINER), RMContainerEventType.EXPIRE);
		}
			break;
		default:
			LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
		}
	}

	private void containerLaunchedOnNode(ContainerId containerId, FiCaSchedulerNode node) {
		// Get the application for the finished container
		ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();
		FiCaSchedulerApp application = getApplication(applicationAttemptId);
		if (application == null) {
			LOG.info("Unknown application: " + applicationAttemptId + " launched container " + containerId + " on node: " + node);
			// Some unknown container sneaked into the system. Kill it.
			this.rmContext.getDispatcher().getEventHandler().handle(new RMNodeCleanContainerEvent(node.getNodeID(), containerId));

			return;
		}

		application.containerLaunchedOnNode(containerId, node.getNodeID());
	}

	@Lock(FifoScheduler.class)
	private synchronized void containerCompleted(RMContainer rmContainer, ContainerStatus containerStatus, RMContainerEventType event) {
		if (rmContainer == null) {
			LOG.info("Null container completed...");
			return;
		}

		// Get the application for the finished container
		Container container = rmContainer.getContainer();
		ApplicationAttemptId applicationAttemptId = container.getId().getApplicationAttemptId();
		FiCaSchedulerApp application = getApplication(applicationAttemptId);

		// Get the node on which the container was allocated
		FiCaSchedulerNode node = getNode(container.getNodeId());

		if (application == null) {
			LOG.info("Unknown application: " + applicationAttemptId + " released container " + container.getId() + " on node: " + node + " with event: " + event);
			return;
		}

		// Inform the application
		application.containerCompleted(rmContainer, containerStatus, event);

		// Inform the node
		node.releaseContainer(container);

		// Update total usage
		Resources.subtractFrom(usedResource, container.getResource());

		LOG.info("Application " + applicationAttemptId + " released container " + container.getId() + " on node: " + node + " with event: " + event);

	}

	private Resource clusterResource = recordFactory.newRecordInstance(Resource.class);
	private Resource usedResource = recordFactory.newRecordInstance(Resource.class);

	private synchronized void removeNode(RMNode nodeInfo) {
		FiCaSchedulerNode node = getNode(nodeInfo.getNodeID());
		if (node == null) {
			return;
		}
		// Kill running containers
		for (RMContainer container : node.getRunningContainers()) {
			containerCompleted(container, SchedulerUtils.createAbnormalContainerStatus(container.getContainerId(), SchedulerUtils.LOST_CONTAINER), RMContainerEventType.KILL);
		}

		// Remove the node
		this.nodes.remove(nodeInfo.getNodeID());

		// Update cluster metrics
		Resources.subtractFrom(clusterResource, node.getRMNode().getTotalCapability());
		flowNetworkMonitor.removeNode(node);
	}

	@Override
	public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues, boolean recursive) {
		return DEFAULT_QUEUE.getQueueInfo(false, false);
	}

	@Override
	public List<QueueUserACLInfo> getQueueUserAclInfo() {
		return DEFAULT_QUEUE.getQueueUserAclInfo(null);
	}

	private synchronized void addNode(RMNode nodeManager) {
		FiCaSchedulerNode node = new FiCaSchedulerNode(nodeManager);
		this.nodes.put(nodeManager.getNodeID(), node);
		Resources.addTo(clusterResource, nodeManager.getTotalCapability());
		flowNetworkMonitor.addNode(node);
	}

	@Override
	public void recover(RMState state) {
		// NOT IMPLEMENTED
	}

	@Override
	public synchronized SchedulerNodeReport getNodeReport(NodeId nodeId) {
		FiCaSchedulerNode node = getNode(nodeId);
		return node == null ? null : new SchedulerNodeReport(node);
	}

	private RMContainer getRMContainer(ContainerId containerId) {
		FiCaSchedulerApp application = getApplication(containerId.getApplicationAttemptId());
		return (application == null) ? null : application.getRMContainer(containerId);
	}

	@Override
	public QueueMetrics getRootQueueMetrics() {
		return DEFAULT_QUEUE.getMetrics();
	}

	private List<FiCaSchedulerNode> getAvailableNodes() {
		List<FiCaSchedulerNode> availableNodes = new ArrayList<FiCaSchedulerNode>();
		for (FiCaSchedulerNode node : nodes.values()) {
			// if (node.getAvailableResource().getMemory() > 0) {
			// TODO: might have problems
			if (node.getAvailableResource().getMemory() / 512 > 0) {
				// LOG.fatal("[Available] node: " + node.getHostName() + " -> "
				// + "running: " + node.getRunningContainers().size() +
				// ", used: " + node.getUsedResource() + ", available: "+
				// node.getAvailableResource());
				availableNodes.add(node);
			}
		}
		return availableNodes;
	}

	private List<FiCaSchedulerApp> getSchedulableApps() {
		List<FiCaSchedulerApp> apps = new ArrayList<FiCaSchedulerApp>();
		for (Map.Entry<ApplicationAttemptId, FiCaSchedulerApp> e : applications.entrySet()) {
			FiCaSchedulerApp application = e.getValue();
			for (Priority priority : application.getPriorities()) {
				int maxContainers = application.getResourceRequest(priority, FiCaSchedulerNode.ANY).getNumContainers();
				if (maxContainers > 0) {
					RMApp rmApp = rmContext.getRMApps().get(application.getApplicationId());
					String appName = rmApp != null ? rmApp.getName() : "";
					LOG.fatal("[Schedulable] " + application.getApplicationAttemptId() + " -> name: " + appName + ", priority:" + priority + ", required: " + maxContainers + " containers");
					apps.add(application);
				}
			}
		}
		return apps;
	}

	private void printStatus() {
		// flowNetworkMonitor.update(flownetwork_period);
		flowNetworkMonitor.printNodeStatus();

		LOG.fatal("----------------------------------------");
		LOG.fatal("Resource request table");
		LOG.fatal("----------------------------------------");
		for (FiCaSchedulerApp app : applications.values()) {
			showRequests(app);
		}
	}

	private synchronized void updateFlowNetwork() {
		// LOG.fatal("#################################################");
		// LOG.fatal("# Update flow network: " + applications.size());
		// LOG.fatal("# cluster: " + clusterResource + ", used: " +
		// usedResource);
		// LOG.fatal("#################################################");

		List<FiCaSchedulerNode> availableNodes = getAvailableNodes();
		List<FiCaSchedulerApp> schedulableApps = getSchedulableApps();

		if (availableNodes.size() == 0 || schedulableApps.size() == 0) {
			// LOG.fatal("[Scheduler] no need to schedule");
			return;
		}

		// Ready to assign tasks onto nodes
		List<Assignment> assignments = null;
		List<FlowNetworkNode> nodes = flowNetworkMonitor.getNodes();
		List<FlowNetworkTask> tasks = flowNetworkMonitor.buildUnscheduledTasks(new LinkedList<FiCaSchedulerApp>(applications.values()));
		List<FlowNetworkStorage> storageNodes = flowNetworkMonitor.getStorageNodes();

		if (tasks.size() == 0) {
			LOG.fatal("[Scheduler] no more tasks to scheduel");
			return;
		}

		String modelName = ((FlowSchedulerConfiguration) getConf()).getAssignmentModel();
		AssignmentModel model;
		if (modelName.equals("Balancing")) {
			model = new BalancingModel(nodes, tasks);
			LOG.fatal("[Assignment] model: Balancing Model");
		} else if (modelName.equals("Fifo")) {
			model = new FifoModel(nodes, tasks);
			LOG.fatal("[Assignment] model: Fifo Model");
		} else if (modelName.equals("Bulk")) {
			model = new BulkModel(nodes, tasks);
			LOG.fatal("[Assignment] model: Bulk Model");
		} else {
			model = new MinCostFlowModel(nodes, storageNodes, tasks, solver);
			LOG.fatal("[Assignment] model: Min Cost Flow Model");
		}

		LOG.fatal("[Assignment] model: " + model.getClass());

		model.construct();
		List<Solution> solutions = model.solve();
		assignments = model.constructAssignments(solutions);

		LOG.fatal("[Scheduler] # of assignments: " + assignments.size());

		printAssignmentResults(assignments);

		// Allocate containers to tasks
		// TODO: retrieve assignment result, assigning...
		for (Assignment assignment : assignments) {
			FiCaSchedulerNode node = assignment.getNode().node;
			FiCaSchedulerApp application = assignment.getTask().app.app;
			int assignableContainers = 1;
			// Priority priority =
			// org.apache.hadoop.yarn.server.resourcemanager.resource.Priority.create(assignment.task.getPriority());
			Priority priority = assignment.task.getPriority();
			ResourceRequest request = null;
			// TODO: smart here...hard to distinguish the type...
			// Request from tasks are not correct because
			// FlowNetworkMonitor.getTasks return them with .ANY constraints
			NodeType type = NodeType.OFF_SWITCH;
			if (priority.getPriority() == 0) {
				type = NodeType.OFF_SWITCH;
				request = application.getResourceRequest(priority, FiCaSchedulerNode.ANY);
			} else if (priority.getPriority() == 20 || priority.getPriority() == 5) {
				// TODO: no input data should not be RACK
				type = NodeType.RACK_LOCAL;
				request = application.getResourceRequest(priority, node.getRMNode().getRackName());
				if (request == null) {
					type = NodeType.OFF_SWITCH;
					request = application.getResourceRequest(priority, FiCaSchedulerNode.ANY);
				}
			} else if (priority.getPriority() == 10) {
				type = NodeType.OFF_SWITCH;
				request = application.getResourceRequest(priority, FiCaSchedulerNode.ANY);
			}
			// tasks can be launched during scheduling period and this case can
			// happen
			if (request != null) {
				assignContainer(node, application, priority, assignableContainers, request, type, assignment.getTask());
			}
		}

		// Update cluster resource usage in application's view
		for (FiCaSchedulerApp application : applications.values()) {
			application.setHeadroom(Resources.subtract(clusterResource, usedResource));
		}

		metrics.setAvailableResourcesToQueue(Resources.subtract(clusterResource, usedResource));

	}

	private void printAssignmentResults(List<Assignment> assignments) {
		Map<FlowNetworkNode, List<FlowNetworkTask>> assignmentMap = new HashMap<FlowNetworkMonitor.FlowNetworkNode, List<FlowNetworkMonitor.FlowNetworkTask>>();
		for (FlowNetworkNode node : flowNetworkMonitor.getNodes()) {
			assignmentMap.put(node, new LinkedList<FlowNetworkMonitor.FlowNetworkTask>());
		}
		for (Assignment assignment : assignments) {
			List<FlowNetworkTask> tasks = assignmentMap.get(assignment.node);
			tasks.add(assignment.task);
			assignmentMap.put(assignment.node, tasks);
		}

		for (FlowNetworkNode node : flowNetworkMonitor.getNodes()) {
			LOG.fatal("[] " + node);
			for (FlowNetworkTask task : assignmentMap.get(node)) {
				LOG.fatal("\t<new> " + task);
			}
		}
	}

	synchronized private void showRequests(FiCaSchedulerApp app) {
		LOG.fatal("Application: " + app.getApplicationAttemptId() + " -> running: " + app.getLiveContainers().size() + ", consumed: " + app.getCurrentConsumption());
		for (Priority priority : app.getPriorities()) {
			Map<String, ResourceRequest> requests = app.getResourceRequests(priority);
			if (requests != null) {
				LOG.fatal("\t@ priority=" + priority.getPriority() + " => required=" + app.getTotalRequiredResources(priority));
				for (Entry<String, ResourceRequest> request : requests.entrySet()) {
					LOG.fatal("\t# " + request.getKey() + ": " + request.getValue() + " => " + request.getValue().getNumContainers());
				}
			}
		}
	}

	public int flownetwork_period = 5;
	private ScheduledThreadPoolExecutor updatorPool;
	private FlowNetworkMonitor flowNetworkMonitor;
	private CS2Solver solver;

}
