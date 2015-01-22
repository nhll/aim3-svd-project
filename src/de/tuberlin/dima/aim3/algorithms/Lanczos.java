package de.tuberlin.dima.aim3.algorithms;

import de.tuberlin.dima.aim3.datatypes.LanczosResult;
import org.apache.mahout.math.MatrixWritable;

public final class Lanczos {

  private Lanczos() {
    // Private constructor pretty much makes this class static.
  }

  public static LanczosResult process(MatrixWritable A, Long m) {
    MatrixWritable Tmm = new MatrixWritable();
    MatrixWritable Vm = new MatrixWritable();

    // TODO: Implement Lanczos algorithm!

    return new LanczosResult(Tmm, Vm);
  }
}
