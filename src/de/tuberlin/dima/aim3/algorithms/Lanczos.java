package de.tuberlin.dima.aim3.algorithms;

import de.tuberlin.dima.aim3.datatypes.LanczosResult;
import de.tuberlin.dima.aim3.operators.DotProduct;
import de.tuberlin.dima.aim3.operators.RejectAll;
import de.tuberlin.dima.aim3.operators.VectorElementsToSingleVector;
import de.tuberlin.dima.aim3.operators.VectorIndexFilter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.mahout.math.MatrixWritable;

import java.util.ArrayList;

public final class Lanczos {

  private Lanczos() {
    // Private constructor pretty much makes this class static.
  }

  public static LanczosResult process(DataSet<Tuple2<Integer, Double[]>> A, int m) {
    ExecutionEnvironment env = A.getExecutionEnvironment();

    MatrixWritable Tmm = new MatrixWritable();
    MatrixWritable Vm = new MatrixWritable();

    ArrayList<Tuple2<Integer, Double>> aList   = new ArrayList<Tuple2<Integer, Double>>();
    ArrayList<Tuple2<Integer, Double>> bList   = new ArrayList<Tuple2<Integer, Double>>();
    ArrayList<Tuple2<Integer, Double[]>> vList = new ArrayList<Tuple2<Integer, Double[]>>();
    ArrayList<Tuple2<Integer, Double[]>> wList = new ArrayList<Tuple2<Integer, Double[]>>();

    // Prepare a 0-vector with m elements.
    Double[] zeroVector = new Double[m];
    for (int i = 0; i < m; i++) {
      zeroVector[i] = 0.0;
    }

    vList.add(0, new Tuple2<Integer, Double[]>(0, zeroVector)); // v[0] <-- 0-vector
    vList.add(1, new Tuple2<Integer, Double[]>(1, zeroVector)); // TODO: Use random vector with norm 1 instead!
    bList.add(0, new Tuple2<Integer, Double>(0, 0.0));
    bList.add(1, new Tuple2<Integer, Double>(1, 0.0));

    // Add an element to the a and w array lists, because you can't create DataSets from empty collections... These will
    // be filtered out by the RejectAll filter below.
    aList.add(new Tuple2<Integer, Double>(0, 0.0));
    wList.add(new Tuple2<Integer, Double[]>(0, new Double[0]));

    Tuple2<Integer, Double> testTuple = new Tuple2<Integer, Double>(0, 0.0);

    // Convert everything to data sets. For a and w, use the RejectAll filter to produce empty DataSets by filtering out
    // all elements.
    DataSet<Tuple2<Integer, Double>> a   = env.fromCollection(aList).filter(new RejectAll<Tuple2<Integer, Double>>());
    DataSet<Tuple2<Integer, Double>> b   = env.fromCollection(bList);
    DataSet<Tuple2<Integer, Double[]>> v = env.fromCollection(vList);
    DataSet<Tuple2<Integer, Double[]>> w = env.fromCollection(wList).filter(new RejectAll<Tuple2<Integer, Double[]>>());

    // Within this loop, a, b, v and w have to be DataSets of some kind. Otherwise we're not able to properly process
    // them with Flink. The problem is that in these DataSets, we have to preserve information about each element's
    // position. For example, if a would be a DataSet<Double>, then we wouldn't know which iteration j a specific Double
    // value in that set belongs to...
    //
    // --> Use Tuple2s containing the corresponding index for each element?
    //     --> Not optimal; we still have to iterate over the tuples until we find the one we want.
    for (int j = 1; j < m; j++) {
      // Get vj by filtering out all v vectors with an index != j.
      DataSet<Tuple2<Integer, Double[]>> vj = v.filter(new VectorIndexFilter(j));

      // TODO: w[j]   <-- A    * v[j]
      DataSet<Tuple2<Integer, Double[]>> wj = A.groupBy(0).reduceGroup(new DotProduct())
                                               .withBroadcastSet(vj, "otherVector")
                                               .reduceGroup(new VectorElementsToSingleVector(j));
      w = w.union(wj);

      // TODO: a[j]   <-- w[j] * v[j]
      // TODO: w[j]   <-- w[j] - a[j] * v[j] - b[j] * v[j-1]
      // TODO: b[j+1] <-- l2norm(w[j])
      // TODO: v[j+1] <-- w[j] / b[j+1]

      // TODO: If v[j+1] is not orthogonal to v[j] OR v[j+1] already exists in v, mark v[j+1] as "spurious".
    }

    w.writeAsText("data/w.out");

    // TODO: wm <-- A  * vm
    // TODO: am <-- wm * vm

    return new LanczosResult(Tmm, Vm);
  }
}
