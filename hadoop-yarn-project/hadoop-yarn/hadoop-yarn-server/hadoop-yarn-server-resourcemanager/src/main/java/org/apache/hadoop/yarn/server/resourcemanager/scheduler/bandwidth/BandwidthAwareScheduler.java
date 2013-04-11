package org.apache.hadoop.yarn.server.resourcemanager.scheduler.bandwidth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
import org.apache.hadoop.util.RunJar;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.bandwidth.FlowNetwork.Assignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.bandwidth.FlowNetwork.Solution;
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
import org.apache.hadoop.yarn.util.BuilderUtils;

@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class BandwidthAwareScheduler extends FifoScheduler implements ResourceScheduler, Configurable {
	private static final Log LOG = LogFactory.getLog(BandwidthAwareScheduler.class);

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
		BandwidthAwareSchedulerConfiguration newConf = new BandwidthAwareSchedulerConfiguration(conf);
		setConf(newConf);
		if (!this.initialized) {
			this.rmContext = rmContext;
			this.minimumAllocation = Resources.createResource(conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
			this.maximumAllocation = Resources.createResource(conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB, YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB));
			this.metrics = QueueMetrics.forQueue(DEFAULT_QUEUE_NAME, null, false, conf);
			this.activeUsersManager = new ActiveUsersManager(metrics);
			this.initialized = true;

			this.flownetwork_period = newConf.getFlowNetworkPeriod();
			this.updatorPool = new ScheduledThreadPoolExecutor(flownetwork_period);
			this.containerRequestMap = new HashMap<RMContainer, ResourceRequest>();
			this.flowNetwork = new FlowNetwork(rmContext, newConf, containerRequestMap);

			updatorPool.scheduleAtFixedRate(new Runnable() {

				@Override
				public void run() {
					try {
						updateFlowNetwork();
					} catch (Throwable t) {
						LOG.fatal("MinCostFlow module worked abnormally!!!", t);
					}
				}
			}, 10, 5, TimeUnit.SECONDS);
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
		// LOG.fatal("Allocate resources: " + application.getApplicationId() + " -> ");
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
				// LOG.fatal("allocate:" + " applicationId=" + applicationAttemptId + " #ask=" + ask.size());
				// LOG.fatal("allocate: pre-update" + " applicationId=" + applicationAttemptId + " application=" + application.getApplicationId());
				// application.showRequests();
				// LOG.fatal("Before updating ask requests");
				// showRequests(application);

				// Update application requests
				application.updateResourceRequests(ask);

				// LOG.fatal("allocate: post-update" + " applicationId=" + applicationAttemptId + " application=" + application.getApplicationId());
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
		flowNetwork.addApp(schedulerApp);
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

		flowNetwork.removeApp(application);
	}

	/**
	 * Heart of the scheduler...
	 * 
	 * @param node
	 *            node on which resources are available to be allocated
	 */
	private void assignContainers(FiCaSchedulerNode node) {
		LOG.debug("assignContainers:" + " node=" + node.getRMNode().getNodeAddress() + " #applications=" + applications.size());

		// List<FiCaSchedulerApp> appList = new ArrayList<FiCaSchedulerApp>(applications.values());
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
		LOG.fatal("assignContainers:" + " node=" + node.getRMNode().getNodeAddress() + " application=" + application.getApplicationId().getId() + " priority=" + priority.getPriority()
				+ " assignableContainers=" + assignableContainers + " request=" + request + " type=" + type);
		Resource capability = request.getCapability();

		int availableContainers = node.getAvailableResource().getMemory() / capability.getMemory(); // TODO: A buggy
		int assignedContainers = Math.min(assignableContainers, availableContainers);

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
				// LOG.fatal("** before **");
				// showRequests(application);
				RMContainer rmContainer = application.allocate(type, node, priority, request, container);
				// LOG.fatal("** after **");
				// showRequests(application);
				containerRequestMap.put(rmContainer, request);
				// Inform the node
				node.allocateContainer(application.getApplicationId(), rmContainer);

				LOG.fatal("$ assign:" + container.getResource() + " on " + node.getHostName() + " to " + application.getApplicationAttemptId());

				// Update usage for this container
				Resources.addTo(usedResource, capability);
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

			flowNetwork.launchTask(node, container, app);
		}

		// Process completed containers
		for (ContainerStatus completedContainer : completedContainers) {
			ContainerId containerId = completedContainer.getContainerId();
			RMContainer container = getRMContainer(containerId);
			FiCaSchedulerApp app = getApplication(container.getApplicationAttemptId());

			LOG.debug("Container FINISHED: " + containerId);
			containerCompleted(container, completedContainer, RMContainerEventType.FINISHED);

			flowNetwork.completeTask(node, container, app);
			containerRequestMap.remove(container);
		}

		if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource, node.getAvailableResource(), minimumAllocation)) {
			// LOG.debug("Node heartbeat " + rmNode.getNodeID() + " available resource = " + node.getAvailableResource());
			// assignContainers(node);
			// LOG.debug("Node after allocation " + rmNode.getNodeID() + " resource = " + node.getAvailableResource());
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
		flowNetwork.removeNode(node);
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
		flowNetwork.addNode(node);
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

	private void printResourceRequestTable() {
		for (FiCaSchedulerApp app : applications.values()) {
			LOG.info(app.getApplicationId() + ": " + app.getUser() + ", " + app.getCurrentConsumption());
			for (RMContainer rmc : app.getLiveContainers()) {
				LOG.info("Live: " + rmc.toString());
			}
			for (RMContainer rmc : app.getReservedContainers()) {
				LOG.info("Reserved: " + rmc.toString());
			}
		}
	}

	private List<FiCaSchedulerNode> getAvailableNodes() {
		List<FiCaSchedulerNode> availableNodes = new ArrayList<FiCaSchedulerNode>();
		for (FiCaSchedulerNode node : nodes.values()) {
			if (node.getAvailableResource().getMemory() > 0) {
				LOG.fatal("[Available] node: " + node.getHostName() + " -> " + "running: " + node.getRunningContainers().size() + ", used: " + node.getUsedResource() + ", available: "
						+ node.getAvailableResource());
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

	private void updateFlowNetwork() {
		LOG.fatal("#################################################");
		LOG.fatal("# Update flow network: " + applications.size());
		LOG.fatal("# cluster: " + clusterResource + ", used: " + usedResource);
		LOG.fatal("#################################################");

		// TODO: check run algorithm -> available nodes and unshceduled tasks, apps
		List<FiCaSchedulerNode> availableNodes = getAvailableNodes();
		List<FiCaSchedulerApp> schedulableApps = getSchedulableApps();

		flowNetwork.update(flownetwork_period);
		flowNetwork.printNodeStatus();

		// TODO: here has some bugs...
		if (availableNodes.size() == 0 || schedulableApps.size() == 0) {
			LOG.fatal("[Scheduler] no need to schedule");
			return;
		}

		LOG.fatal("----------------------------------------");
		LOG.fatal("Resource request table");
		LOG.fatal("----------------------------------------");
		for (FiCaSchedulerApp app : applications.values()) {
			showRequests(app);
		}

		// TODO: build the flow model, update node loading
		flowNetwork.construct();

		// TODO: solve the min cost flow network
		Solution solution = flowNetwork.solve();
		LOG.fatal("[Scheduler] # of assignments: " + solution.assignments.size());

		// TODO: retrieve assignment result, assigning...
		for (Assignment assignment : solution.getAssignments()) {
			FiCaSchedulerNode node = assignment.getNode();
			FiCaSchedulerApp application = assignment.getApp();
			Priority priority = assignment.getPriory();
			int assignableContainers = assignment.getNumber();
			ResourceRequest request = assignment.getRequest();
			// TODO: smart here...hard to distinguish the type...
			NodeType type = NodeType.OFF_SWITCH;

			// LOG.fatal("before......");
			// showRequests(application);
			assignContainer(node, application, priority, assignableContainers, request, type);
			// LOG.fatal("after......");
			// showRequests(application);
		}

		// TODO: miscs
		for (FiCaSchedulerApp application : applications.values()) {
			application.setHeadroom(Resources.subtract(clusterResource, usedResource));
		}

		/**
		 * for (FiCaSchedulerApp app : applications.values()) { RMApp rmapp = this.rmContext.getRMApps().get(app.getApplicationId()); if (rmapp != null) { LOG.fatal("@ " + rmapp.getName());
		 * LOG.fatal("@ " + rmapp.getDiagnostics()); } LOG.fatal("# App: " + app.getApplicationId()); Resource resource_current = app.getCurrentConsumption(); LOG.fatal("# Current: " +
		 * resource_current);
		 * 
		 * for (Priority priority : app.getPriorities()) { int numOfAnyContainers = app.getResourceRequest(priority, FiCaSchedulerNode.ANY).getNumContainers(); LOG.fatal("# Priority=" + priority +
		 * ", Type=*, Requires: " + numOfAnyContainers); Set<String> rackSet = getRackSet(); // LOG.fatal("Total rack number: " + rackSet.size()); for (String rackName : rackSet) { ResourceRequest
		 * request = app.getResourceRequest(priority, rackName); if (request != null) { int numOfRackContainers = request.getNumContainers(); LOG.fatal("# Priority=" + priority + ", Type=" + rackName
		 * + ", Requires: " + numOfRackContainers); } } }
		 * 
		 * for (Priority priority : app.getPriorities()) { for (FiCaSchedulerNode node : availableNodeSet) { int maxContainers = getMaxAllocatableContainers(app, priority, node, NodeType.OFF_SWITCH);
		 * if (maxContainers > 0) { assignContainersOnNode(node, app, priority); if (Resources.lessThan(resourceCalculator, clusterResource, node.getAvailableResource(), minimumAllocation)) { break; }
		 * } } } for (FiCaSchedulerApp application : applications.values()) { application.setHeadroom(Resources.subtract(clusterResource, usedResource)); } }
		 */
	}

	private Set<String> getRackSet() {
		Set<String> racks = new HashSet<String>();
		for (FiCaSchedulerNode node : nodes.values()) {
			// LOG.fatal("## " + node.getHostName() + ", " + node.getRMNode().getNodeAddress() + ": " + node.getRackName());
			racks.add(node.getRMNode().getRackName());
		}
		return racks;
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
	private Map<RMContainer, ResourceRequest> containerRequestMap;
	private FlowNetwork flowNetwork;

}
