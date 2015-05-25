package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

public class FlowRate {
  double flowIn;
  double flowOut;

  public FlowRate(double flowIn, double flowOut) {
    super();
    this.flowIn = flowIn;
    this.flowOut = flowOut;
  }

  public double getFlowIn() {
    return flowIn;
  }

  public void setFlowIn(double flowIn) {
    this.flowIn = flowIn;
  }

  public double getFlowOut() {
    return flowOut;
  }

  public void setFlowOut(double flowOut) {
    this.flowOut = flowOut;
  }

  @Override
  public String toString() {
    return "FlowRate [flowIn=" + flowIn + ", flowOut=" + flowOut + "]";
  }

}