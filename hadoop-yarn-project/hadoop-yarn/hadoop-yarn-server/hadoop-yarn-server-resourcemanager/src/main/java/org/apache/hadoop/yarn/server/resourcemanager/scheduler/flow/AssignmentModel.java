package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.List;

public interface AssignmentModel {

	public void construct();

	public void solve();

	public List<Assignment> constructAssignments();
}
