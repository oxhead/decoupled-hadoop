package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

public class Task {
	enum Type {
		AppMaster, Map, Reduce, UNKNOWN
	};
}
