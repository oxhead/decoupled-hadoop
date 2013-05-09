package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.MinCostFlowModel.Node;

public interface NetworkFlowSolver {

	public List<Solution> solve(MinCostFlowModel.Network networkModel);

	class Solution {

		Node head;
		Node tail;
		int flow;

		public Solution(Node head, Node tail, int flow) {
			this.head = head;
			this.tail = tail;
			this.flow = flow;
		}

		@Override
		public String toString() {
			return "Solution [head=" + head + ", tail=" + tail + ", flow=" + flow + "]";
		}

	}
}
