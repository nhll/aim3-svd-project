package de.tuberlin.dima.aim3.algorithms;

import de.tuberlin.dima.aim3.datatypes.LanczosResult;
import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.operators.DotProduct;
import de.tuberlin.dima.aim3.operators.RejectAll;
import de.tuberlin.dima.aim3.operators.VectorElementsToSingleVector;
import de.tuberlin.dima.aim3.operators.VectorIndexFilter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.mahout.math.MatrixWritable;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;

public final class Lanczos {

  private Lanczos() {
    // Private constructor pretty much makes this class static.
  }

  public static void process(DataSet<Vector> A, int m) {
    ExecutionEnvironment env = A.getExecutionEnvironment();

    ArrayList<Tuple2<Integer, Double>> aList = new ArrayList<Tuple2<Integer, Double>>();
    ArrayList<Tuple2<Integer, Double>> bList = new ArrayList<Tuple2<Integer, Double>>();
    ArrayList<Vector> vList = new ArrayList<Vector>();
    ArrayList<Vector> wList = new ArrayList<Vector>();

    vList.add(Vector.getZeroVector(3, 0));             // v[0] <-- 0-vector
    vList.add(Vector.getRandomVector(3, 1, 1));        // v[1] <-- Random vector with norm 1
    bList.add(0, new Tuple2<Integer, Double>(0, 0.0)); // b[0] <-- 0
    bList.add(1, new Tuple2<Integer, Double>(1, 0.0)); // b[1] <-- 0

    // Add an element to the a and w array lists, because you can't create DataSets from empty collections... These will
    // be filtered out by the RejectAll filter below.
    aList.add(new Tuple2<Integer, Double>(0, 0.0));
    wList.add(Vector.getZeroVector(0));

    // Convert everything to data sets. For a and w, use the RejectAll filter to produce empty DataSets by filtering out
    // all elements.
    DataSet<Tuple2<Integer, Double>> a = env.fromCollection(aList).filter(new RejectAll<Tuple2<Integer, Double>>());
    DataSet<Tuple2<Integer, Double>> b = env.fromCollection(bList);
    DataSet<Vector> v = env.fromCollection(vList);
    DataSet<Vector> w = env.fromCollection(wList).filter(new RejectAll<Vector>());

    // Within this loop, a, b, v and w have to be DataSets of some kind. Otherwise we're not able to properly process
    // them with Flink. The problem is that in these DataSets, we have to preserve information about each element's
    // position. For example, if a would be a DataSet<Double>, then we wouldn't know which iteration j a specific Double
    // value in that set belongs to...
    //
    // --> Use Tuple2s containing the corresponding index for each element?
    //     --> Not optimal; we still have to iterate over the tuples until we find the one we want.
    for (int j = 1; j < m; j++) {
      // Get vj by filtering out all v vectors with an index != j.
      DataSet<Vector> vj = v.filter(new VectorIndexFilter(j));

      // TODO: w[j]   <-- A    * v[j]
      DataSet<Vector> wj = A.groupBy("index").reduceGroup(new DotProduct())
                            .withBroadcastSet(vj, "otherVector")
                            .reduceGroup(new VectorElementsToSingleVector(j));
      // TODO: Append wj to w!

      vj.writeAsText(new File("data/out/v" + j + ".out").getAbsolutePath(), FileSystem.WriteMode.OVERWRITE);
      wj.writeAsText(new File("data/out/w" + j + ".out").getAbsolutePath(), FileSystem.WriteMode.OVERWRITE);

      // TODO: a[j]   <-- w[j] * v[j]
      // TODO: w[j]   <-- w[j] - a[j] * v[j] - b[j] * v[j-1]
      // TODO: b[j+1] <-- l2norm(w[j])
      // TODO: v[j+1] <-- w[j] / b[j+1]

      // TODO: If v[j+1] is not orthogonal to v[j] OR v[j+1] already exists in v, mark v[j+1] as "spurious".
    }

    // TODO: wm <-- A  * vm
    // TODO: am <-- wm * vm

    // TODO: Return something useful! Probably two data sets containing Tmm and Vm, respectively...
  }
}
