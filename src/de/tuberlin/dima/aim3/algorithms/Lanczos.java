package de.tuberlin.dima.aim3.algorithms;

import de.tuberlin.dima.aim3.datatypes.*;
import de.tuberlin.dima.aim3.operators.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;

import java.io.File;
import java.util.ArrayList;

public final class Lanczos {

  private Lanczos() {
    // Private constructor pretty much makes this class static.
  }

  public static void process(DataSet<Vector> A, int m) {
    ExecutionEnvironment env = A.getExecutionEnvironment();

    ArrayList<VectorElement> aList = new ArrayList<VectorElement>();
    ArrayList<VectorElement> bList = new ArrayList<VectorElement>();
    ArrayList<Vector>        vList = new ArrayList<Vector>();
    ArrayList<Vector>        wList = new ArrayList<Vector>();

    vList.add(Vector.getZeroVector(3, 0));      // v[0] <-- 0-vector
    vList.add(Vector.getRandomVector(3, 1, 1)); // v[1] <-- Random vector with norm 1
    bList.add(new VectorElement(0, 0.0));       // b[0] <-- 0.0
    bList.add(new VectorElement(1, 0.0));       // b[1] <-- 0.0

    // Add an element to the a and w array lists, because you can't create DataSets from empty collections... These will
    // be filtered out by the RejectAll filter below.
    aList.add(new VectorElement(0, 0.0));
    wList.add(Vector.getZeroVector(0));

    // Convert everything to data sets. For a and w, use the RejectAll filter to produce empty DataSets by filtering out
    // all elements.
    DataSet<VectorElement> a = env.fromCollection(aList).filter(new RejectAll<VectorElement>());
    DataSet<VectorElement> b = env.fromCollection(bList);
    DataSet<Vector>        v = env.fromCollection(vList);
    DataSet<Vector>        w = env.fromCollection(wList).filter(new RejectAll<Vector>());

    for (int j = 1; j < m; j++) {
      // Get vj by filtering out all v vectors with an index != j.
      DataSet<Vector> vj = v.filter(new VectorIndexFilter(j));

      // w[j] <-- A * v[j]
      DataSet<Vector> wj = A.groupBy("index").reduceGroup(new DotProduct())
                            .withBroadcastSet(vj, "otherVector")
                            .reduceGroup(new VectorElementsToSingleVector(j));
      // Don't append wj to w yet because it will be modified again later in this iteration.

      // a[j] <-- w[j] * v[j]
      DataSet<VectorElement> aj = wj.reduceGroup(new DotProduct())
                                    .withBroadcastSet(vj, "otherVector");
      a = a.union(aj);


      // TODO: w[j]   <-- w[j] - a[j] * v[j] - b[j] * v[j-1]
      w = w.union(wj);

      // TODO: b[j+1] <-- l2norm(w[j])

      // TODO: v[j+1] <-- w[j] / b[j+1]

      // TODO: If v[j+1] is not orthogonal to v[j] OR v[j+1] already exists in v, mark v[j+1] as "spurious".

      // TODO: Remove test outputs!
      vj.writeAsText(new File("data/out/v" + j + ".out").getAbsolutePath(), FileSystem.WriteMode.OVERWRITE);
      wj.writeAsText(new File("data/out/w" + j + ".out").getAbsolutePath(), FileSystem.WriteMode.OVERWRITE);
      aj.writeAsText(new File("data/out/a" + j + ".out").getAbsolutePath(), FileSystem.WriteMode.OVERWRITE);
    }

    w.writeAsText(new File("data/out/w.out").getAbsolutePath(), FileSystem.WriteMode.OVERWRITE);
    a.writeAsText(new File("data/out/a.out").getAbsolutePath(), FileSystem.WriteMode.OVERWRITE);

    // TODO: wm <-- A  * vm
    // TODO: am <-- wm * vm

    // TODO: Return something useful! Probably two data sets containing Tmm and Vm, respectively...
  }
}
