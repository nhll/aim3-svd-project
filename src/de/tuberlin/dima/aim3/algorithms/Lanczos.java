package de.tuberlin.dima.aim3.algorithms;

import de.tuberlin.dima.aim3.datatypes.LanczosResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;

import java.util.ArrayList;

public final class Lanczos {

  private Lanczos() {
    // Private constructor pretty much makes this class static.
  }

  public static LanczosResult process(DataSet<Tuple2<Long, Double[]>> A, int m) {
    MatrixWritable Tmm = new MatrixWritable();
    MatrixWritable Vm = new MatrixWritable();

    ArrayList<Double> a = new ArrayList<Double>(m);
    ArrayList<Double> b = new ArrayList<Double>(m);
    ArrayList<Vector> v = new ArrayList<Vector>(m);
    ArrayList<Vector> w = new ArrayList<Vector>(m);

    v.set(0, new DenseVector(m).assign(0)); // v[0] <-- 0-vector
    v.set(1, new DenseVector(m).assign(1)); // v[1] <-- Random vector with norm 1; TODO: Actually generate such a vector
    b.set(1, 0.0);                          // b[1] <-- 0

    for (int j = 1; j < m; j++) {
      // TODO: w[j]   <-- A    * v[j]
      // TODO: a[j]   <-- w[j] * v[j]
      // TODO: w[j]   <-- w[j] - a[j] * v[j] - b[j] * v[j-1]
      // TODO: b[j+1] <-- l2norm(w[j])
      // TODO: v[j+1] <-- w[j] / b[j+1]

      // TODO: If v[j+1] is not orthogonal to v[j] OR v[j+1] already exists in v, mark v[j+1] as "spurious".
    }

    // TODO: wm <-- A  * vm
    // TODO: am <-- wm * vm

    return new LanczosResult(Tmm, Vm);
  }
}
