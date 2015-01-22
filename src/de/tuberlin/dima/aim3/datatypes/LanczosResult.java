package de.tuberlin.dima.aim3.datatypes;

import org.apache.mahout.math.MatrixWritable;

public class LanczosResult {
  private MatrixWritable Tmm;
  private MatrixWritable Vm;

  public LanczosResult(MatrixWritable Tmm, MatrixWritable Vm) {
    this.Tmm = Tmm;
    this.Vm = Vm;
  }

  public MatrixWritable getTmm() {
    return Tmm;
  }

  public MatrixWritable getVm() {
    return Vm;
  }
}
