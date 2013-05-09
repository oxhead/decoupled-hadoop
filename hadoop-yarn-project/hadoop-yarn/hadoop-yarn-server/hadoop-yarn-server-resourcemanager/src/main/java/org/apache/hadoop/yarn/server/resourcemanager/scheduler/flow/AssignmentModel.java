package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.NetworkFlowSolver.Solution;

public interface AssignmentModel {

	public void construct();

	public List<Solution> solve();

	public List<Assignment> constructAssignments(List<Solution> solutions);
}
