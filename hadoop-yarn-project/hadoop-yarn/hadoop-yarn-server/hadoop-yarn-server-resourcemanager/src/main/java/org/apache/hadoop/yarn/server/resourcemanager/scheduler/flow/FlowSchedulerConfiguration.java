package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.FlowSchedulerTask.Type;

public class FlowSchedulerConfiguration extends Configuration {

  private static final Log LOG = LogFactory.getLog(FlowSchedulerConfiguration.class);

  private static final String CONFIGURATION_FILE = "flow-scheduler.xml";

  @Private
  public static final String PREFIX = "yarn.scheduler.flow.";

  @Private
  public static final String DOT = ".";

  @Private
  public static final String SOLVER = PREFIX + "solver";

  @Private
  public static final String ASSIGNMENT_MODEL = PREFIX + "assignment.model";

  @Private
  public static final String FLOWNETWORK_PERIOD = PREFIX + "flownetwork.period";

  @Private
  public static final String FLOWRATE_TERASORT = PREFIX + "flowrate.terasort";

  @Private
  public static final String FLOWRATE_WORDCOUNT = PREFIX + "flowrate.wordcount";

  @Private
  public static final String FLOWRATE_GREP = PREFIX + "flowrate.grep";

  @Private
  public static final String FLOWRATE_NOCOMPUTATION = PREFIX + "flowrate.nocomputation";

  @Private
  public static final String FLOWRATE_CUSTOMMAP = PREFIX + "flowrate.custommap";

  @Private
  public static final String FLOWRATE_HISTOGRAMMOVIES = PREFIX + "flowrate.histogram-movies";

  @Private
  public static final String FLOWRATE_HISTOGRAMRATINGS = PREFIX + "flowrate.histogram-ratings";

  @Private
  public static final String FLOWRATE_INVERTEDINDEX = PREFIX + "flowrate.inverted-index";

  @Private
  public static final String FLOWRATE_DEFAULT = PREFIX + "flowrate.default";

  @Private
  public static final String FLOWRATE_NODES = PREFIX + "flowrate.nodes";

  public FlowSchedulerConfiguration() {
    this(new Configuration());
  }

  public FlowSchedulerConfiguration(Configuration configuration) {
    super(configuration);
    addResource(CONFIGURATION_FILE);
  }

  public enum Job {
    TERASORT, WORDCOUNT, GREP, NOCOMPUTATION, DEFAULT
  }

  public String getSolverPath() {
    return get(SOLVER);
  }

  public String getAssignmentModel() {
    return get(ASSIGNMENT_MODEL);
  }

  public FlowRate getFlowRate(String jobName, Type type) {
    String[] flowRateString;

    if (jobName.contains("TeraSort")) {
      flowRateString = getStrings(FLOWRATE_TERASORT);
    } else if (jobName.contains("word count")) {
      flowRateString = getStrings(FLOWRATE_WORDCOUNT);
    } else if (jobName.contains("grep-search")) {
      flowRateString = getStrings(FLOWRATE_GREP);
    } else if (jobName.contains("nocomputation")) {
      flowRateString = getStrings(FLOWRATE_NOCOMPUTATION);
    } else if (jobName.contains("CustomMap")) {
      if (jobName.contains("CustomMap_1")) {
        flowRateString = getStrings(FLOWRATE_CUSTOMMAP + ".1");
      } else if (jobName.contains("CustomMap_2")) {
        flowRateString = getStrings(FLOWRATE_CUSTOMMAP + ".2");
      } else if (jobName.contains("CustomMap_3")) {
        flowRateString = getStrings(FLOWRATE_CUSTOMMAP + ".3");
      } else if (jobName.contains("CustomMap_4")) {
        flowRateString = getStrings(FLOWRATE_CUSTOMMAP + ".4");
      } else if (jobName.contains("CustomMap_5")) {
        flowRateString = getStrings(FLOWRATE_CUSTOMMAP + ".5");
      } else if (jobName.contains("CustomMap_6")) {
        flowRateString = getStrings(FLOWRATE_CUSTOMMAP + ".6");
      } else {
        flowRateString = getStrings(FLOWRATE_CUSTOMMAP + ".1");
      }
    } else if (jobName.contains("histogram-movies")) {
      flowRateString = getStrings(FLOWRATE_HISTOGRAMMOVIES);
    } else if (jobName.contains("histogram-ratings")) {
      flowRateString = getStrings(FLOWRATE_HISTOGRAMRATINGS);
    } else if (jobName.contains("inverted-index")) {
      flowRateString = getStrings(FLOWRATE_INVERTEDINDEX);
    } else {
      flowRateString = getStrings(FLOWRATE_DEFAULT);
    }

    FlowRate flowRate;
    if (type.equals(Type.Map)) {
      flowRate = new FlowRate(Double.parseDouble(flowRateString[0]), Double.parseDouble(flowRateString[1]));
    } else {
      flowRate = new FlowRate(Double.parseDouble(flowRateString[2]), Double.parseDouble(flowRateString[3]));
    }

    return flowRate;

  }

  public FlowRate getFlowRate(Job job, Type type) {
    String[] flowRateString;

    // LOG.fatal("<> flowrate: job=" + job + ", type=" + type);
    if (job.equals(Job.TERASORT)) {
      flowRateString = getStrings(FLOWRATE_TERASORT);
    } else if (job.equals(Job.WORDCOUNT)) {
      flowRateString = getStrings(FLOWRATE_WORDCOUNT);
    } else if (job.equals(Job.GREP)) {
      flowRateString = getStrings(FLOWRATE_GREP);
    } else if (job.equals(Job.NOCOMPUTATION)) {
      flowRateString = getStrings(FLOWRATE_NOCOMPUTATION);
    } else {
      flowRateString = getStrings(FLOWRATE_DEFAULT);
    }

    FlowRate flowRate;
    if (type.equals(Type.Map)) {
      flowRate = new FlowRate(Double.parseDouble(flowRateString[0]), Double.parseDouble(flowRateString[1]));
    } else {
      flowRate = new FlowRate(Double.parseDouble(flowRateString[2]), Double.parseDouble(flowRateString[3]));
    }
    // LOG.fatal("<> read: " + Arrays.toString(flowRateString));
    // LOG.fatal("<> flowrate: " + flowRate);

    return flowRate;

  }

  public int getFlowNetworkPeriod() {
    int period = getInt(FLOWNETWORK_PERIOD, 5);
    return period;
  }

  public double getNodeCapacity(String hostName) {
    String[] capacityList = getStrings(FLOWRATE_NODES);
    double capacity = 30;
    for (String s : capacityList) {
      String split_symbol = ":";
      String host_suffix = s.split(split_symbol)[0];
      double host_capacity = Double.parseDouble(s.split(split_symbol)[1]);
      if (hostName.contains(host_suffix)) {
        capacity = host_capacity;
      }
    }
    return capacity;
  }

}