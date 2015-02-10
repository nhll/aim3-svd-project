package de.tuberlin.dima.aim3.algorithms;

import de.tuberlin.dima.aim3.Config;
import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.datatypes.VectorElement;
import de.tuberlin.dima.aim3.operators.*;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public final class Lanczos {

    private Lanczos() {
        // Private constructor pretty much makes this class static.
    }

    public static void process(DataSet<Vector> A, int m) {
        ExecutionEnvironment env = A.getExecutionEnvironment();

        List<VectorElement> aList = new ArrayList<>();
        List<VectorElement> bList = new ArrayList<>();
        List<Vector> vList = new ArrayList<>();
        List<Vector> wList = new ArrayList<>();

        vList.add(Vector.getZeroVector(3, 0));      // v[0] <-- 0-vector
        vList.add(Vector.getRandomVector(3, 1, 1)); // v[1] <-- Random vector with norm 1
        bList.add(new VectorElement(0, 0.0));       // b[0] <-- 0.0
        bList.add(new VectorElement(1, 0.0));       // b[1] <-- 0.0

        // Add an element to the a and w array lists, because you can't create DataSets from empty collections... These
        // will be filtered out by the filter below.
        aList.add(new VectorElement(0, 0.0));
        wList.add(Vector.getZeroVector(0));

        // Convert everything to data sets. For a and w, produce empty data sets by filtering out all elements.
        DataSet<VectorElement> a = env.fromCollection(aList).filter(element -> false);
        DataSet<VectorElement> b = env.fromCollection(bList);
        DataSet<Vector> v = env.fromCollection(vList);
        DataSet<Vector> w = env.fromCollection(wList).filter(vector -> false);

        for (int i = 1; i <= m; i++) {
            int j = i; // We need the current index as an 'effectively final' value for use in lambda expressions...

            // Get vj by filtering out all v vectors with an index != j.
            DataSet<Vector> vj = v.filter(vector -> vector.getIndex() == j);
            vj.writeAsText(Config.getTmpOutput() + "v_" + i + ".out", FileSystem.WriteMode.OVERWRITE);

            // w[j] <-- A * v[j]
            DataSet<Vector> wj = A.groupBy("index").reduceGroup(new DotProduct())
                    .withBroadcastSet(vj, "otherVector")
                    .reduceGroup(new VectorElementsToSingleVector(i));
            wj.writeAsText(Config.getTmpOutput() + "w_" + i + "_1.out", FileSystem.WriteMode.OVERWRITE);

            // a[j] <-- w[j] * v[j]
            DataSet<VectorElement> aj = wj.reduceGroup(new DotProduct())
                    .withBroadcastSet(vj, "otherVector");
            a = a.union(aj);

            if (i == m) {
                w = w.union(wj);
            }
            else {

                // ajVj <-- a[j] * v[j]
                DataSet<Vector> ajVj = vj.reduceGroup(new VectorScalarMultiplication())
                        .withBroadcastSet(aj, "scalar");

                // bjVjMinus1 <-- b[j] * v[j-1]
                DataSet<Vector> bjVjMinus1 = v.filter(vector -> vector.getIndex() == j - 1)
                        .reduceGroup(new VectorScalarMultiplication())
                        .withBroadcastSet(b.filter(element -> element.getIndex() == j), "scalar");

                // wj <-- wj - ajVj - bjVjMinus1
                // TODO: Is this calculation correct?
                wj = wj.reduceGroup(new VectorSubtraction())
                        .withBroadcastSet(ajVj, "otherVector")
                        .reduceGroup(new VectorSubtraction())
                        .withBroadcastSet(bjVjMinus1, "otherVector");
                w = w.union(wj);

                // b[j+1] <-- l2norm(w[j])
                DataSet<VectorElement> bjPlus1 = wj.reduceGroup(new GetVectorNorm())
                        .withBroadcastSet(env.fromElements(j + 1), "index")
                        .withBroadcastSet(env.fromElements(2), "norm");
                b = b.union(bjPlus1);

                // v[j+1] <-- w[j] / b[j+1]
                DataSet<Vector> vjPlus1 = wj.reduceGroup(new VectorScalarDivision())
                        .withBroadcastSet(bjPlus1, "scalar");

                // orthogonalize:
                // for k .. i do
                //   v[j+1] <-- v[j+1] - (v[k] . v[j+1]) * v[k]
                for(int k=1; k<=j; k++) {
                    int l=k;
                    DataSet<Vector> vk = v.filter(vector -> vector.getIndex() == l);
                    // gram-schmitt
                    vjPlus1 = vjPlus1.cross(vk).with((v1, v2) -> v1.minus(v2.times(v2.dot(v1))));
                }
                v = v.union(vjPlus1);
                v.writeAsText(Config.getTmpOutput() + "v__" + j + ".out", FileSystem.WriteMode.OVERWRITE);

                // TODO: If v[j+1] is not orthogonal to v[j] OR v[j+1] already exists in v, mark v[j+1] as "spurious".

                // TODO: Remove test outputs!
                vj.writeAsText(Config.getTmpOutput() + "v_" + i + ".out", FileSystem.WriteMode.OVERWRITE);
                wj.writeAsText(Config.getTmpOutput() + "w_" + i + "_2.out", FileSystem.WriteMode.OVERWRITE);
                aj.writeAsText(Config.getTmpOutput() + "a_" + i + ".out", FileSystem.WriteMode.OVERWRITE);
                ajVj.writeAsText(Config.getTmpOutput() + "ajVj_" + i + ".out", FileSystem.WriteMode.OVERWRITE);
                bjVjMinus1.writeAsText(Config.getTmpOutput() + "bjVjMinus1_" + i + ".out",
                        FileSystem.WriteMode.OVERWRITE);
                bjPlus1.writeAsText(Config.getTmpOutput() + "bjPlus1_" + i + ".out",
                        FileSystem.WriteMode.OVERWRITE);
                vjPlus1.writeAsText(Config.getTmpOutput() + "vjPlus1_" + i + ".out",
                        FileSystem.WriteMode.OVERWRITE);
            }
        }

        // TODO: wm <-- A  * vm
        // TODO: am <-- wm * vm

        DataSet<Tuple3<Integer,Integer,Double>> dots = v.reduceGroup(new GroupReduceFunction<Vector, Tuple3<Integer,Integer,Double>>() {
            List<Vector> vectors = new ArrayList<Vector>();
            @Override
            public void reduce(Iterable<Vector> values, org.apache.flink.util.Collector<Tuple3<Integer,Integer,Double>> out) throws Exception {
                for(Vector v : values) {
                    vectors.add(v);

                }
                System.out.println(vectors);
                for(Vector v1 : vectors) {
                    for(Vector v2 : vectors) {
                        if(v1.getIndex() <= v2.getIndex()) {
                            Tuple3<Integer, Integer, Double> res = new Tuple3<Integer, Integer, Double>(v1.getIndex(), v2.getIndex(), v1.dot(v2));
                            out.collect(res);
                        }
                    }
                }
            }
        });
        dots.writeAsText(Config.getTmpOutput() + "dots.out", FileSystem.WriteMode.OVERWRITE);

        v.writeAsText(Config.getTmpOutput() + "v.out", FileSystem.WriteMode.OVERWRITE);
        w.writeAsText(Config.getTmpOutput() + "w.out", FileSystem.WriteMode.OVERWRITE);
        a.writeAsText(Config.getTmpOutput() + "a.out", FileSystem.WriteMode.OVERWRITE);
        b.writeAsText(Config.getTmpOutput() + "b.out", FileSystem.WriteMode.OVERWRITE);



        // TODO: Return something useful! Probably two data sets containing Tmm and Vm, respectively...
    }
}