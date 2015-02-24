package de.tuberlin.dima.aim3.algorithms;

import de.tuberlin.dima.aim3.datatypes.LanczosResult;
import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.datatypes.VectorElement;
import de.tuberlin.dima.aim3.operators.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public final class Lanczos {

    private Lanczos() {
        // Private constructor pretty much makes this class static.
    }

    public static LanczosResult process(DataSet<Vector> A, int m) {
        ExecutionEnvironment env = A.getExecutionEnvironment();

        List<VectorElement> aList = new ArrayList<>();
        List<VectorElement> bList = new ArrayList<>();
        List<Vector> vList = new ArrayList<>();
        List<Vector> wList = new ArrayList<>();

        vList.add(Vector.getZeroVector(6, 0));      // v[0] <-- 0-vector
        vList.add(Vector.getRandomVector(6, 1, 1)); // v[1] <-- Random vector with norm 1
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
        DataSet<Double> scaleFactor = env.fromElements(1.0);

        for (int i = 1; i <= m; i++) {
            int j = i; // We need the current index as an 'effectively final' value for use in lambda expressions...

            // Get vj by filtering out all v vectors with an index != j.
            DataSet<Vector> vj = v.filter(vector -> vector.getIndex() == j);

            // w[j] <-- A * v[j]
            DataSet<Vector> wj = A.groupBy("index").reduceGroup(new DotProduct())
                                  .withBroadcastSet(vj, "otherVector")
                                  .reduceGroup(new VectorElementsToSingleVector(i));

            // In the first iteration, store the scale factor.
            if (j == 1) {
                scaleFactor = wj.reduceGroup(new GetVectorNormAsDouble())
                                .withBroadcastSet(env.fromElements(2), "norm");
            } else {
                // Mahout-style scaling of wj: dividing wj by the scale factor.
                wj = wj.reduceGroup(new VectorScalarDivision())
                       .withBroadcastSet(scaleFactor, "scalar");
            }

            // a[j] <-- w[j] * v[j]
            DataSet<VectorElement> aj = wj.reduceGroup(new DotProduct())
                                          .withBroadcastSet(vj, "otherVector");
            a = a.union(aj);

            if (i == m) {
                w = w.union(wj);
            } else {
                // ajVj <-- a[j] * v[j]
                DataSet<Vector> ajVj = vj.reduceGroup(new VectorScalarMultiplication())
                                         .withBroadcastSet(aj, "scalar");

                // bjVjMinus1 <-- b[j] * v[j-1]
                DataSet<Vector> bjVjMinus1 = v.filter(vector -> vector.getIndex() == j - 1)
                                              .reduceGroup(new VectorScalarMultiplication())
                                              .withBroadcastSet(b.filter(element -> element.getIndex() == j), "scalar");

                // wj <-- wj - ajVj - bjVjMinus1
                wj = wj.reduceGroup(new VectorSubtraction())
                       .withBroadcastSet(ajVj, "otherVector")
                       .reduceGroup(new VectorSubtraction())
                       .withBroadcastSet(bjVjMinus1, "otherVector");

                // Orthogonalization according to the mahout source code.
                for (int k = 0; k < j; k++) {
                    final int l = k;

                    // alpha = wj * vk
                    DataSet<Vector> vk = v.filter(vec -> vec.getIndex() == l);
                    DataSet<VectorElement> alpha = wj.reduceGroup(new DotProduct())
                                                     .withBroadcastSet(vk, "otherVector");
                    // wj = wj - alpha * vk
                    DataSet<Vector> alphaVk = vk.reduceGroup(new VectorScalarMultiplication())
                                               .withBroadcastSet(alpha, "scalar");
                    wj = wj.reduceGroup(new VectorSubtraction())
                           .withBroadcastSet(alphaVk, "otherVector");
                }

                w = w.union(wj);

                // b[j+1] <-- l2norm(w[j])
                DataSet<VectorElement> bjPlus1 = wj.reduceGroup(new GetVectorNorm())
                                                   .withBroadcastSet(env.fromElements(j + 1), "index")
                                                   .withBroadcastSet(env.fromElements(2), "norm");
                b = b.union(bjPlus1);

                // v[j+1] <-- w[j] / b[j+1]
                DataSet<Vector> vjPlus1 = wj.reduceGroup(new VectorScalarDivision(true))
                                            .withBroadcastSet(bjPlus1, "scalar");
                v = v.union(vjPlus1);
            }
        }

        // Construct Tmm from the a and b values.
        //
        // First, we will only add the a_i and b_i values to each row vector. In order to add the b_i+1 values
        // to the row vectors as well, we will use a copy of b with all indices decremented, so that we're able
        // to join it properly with the temporary Tmm that only contains a_i and b_i values. During this second
        // join, the last row vector will be omitted, because its index does not match any of the decremented b
        // indices anymore. To work around this, we will filter out the last row of our temporary Tmm matrix and
        // add it to the final Tmm matrix in the end (as the last row wouldn't be changed by the second join anyway).
        DataSet<Vector> Tmm = a.join(b)
                               .where("index").equalTo("index")
                               .with(new AlphaBetaJoiner())
                               .withBroadcastSet(env.fromElements(m), "colCount");

        // Get the temporary Tmm's last row vector so that we don't lose it.
        DataSet<Vector> lastTmmRow = Tmm.filter(vec -> vec.getIndex() == m - 1);

        // Join a copy of the b values with decremented indices with the temporary Tmm matrix in order to add all
        // the b_i+1 values to each row vector. The last row vector of Tmm is lost during this step, so we have to
        // manually re-append it afterwards.
        DataSet<VectorElement> bDecrementedIndices = b.map(e -> new VectorElement(e.getIndex() - 2, e.getValue()));
        Tmm = Tmm.join(bDecrementedIndices)
                 .where("index").equalTo("index")
                 .with(new BetaExtender())
                 .union(lastTmmRow);

        // Return two data sets containing Tmm and the Lanczos vectors, respectively.
        return new LanczosResult(Tmm, v);
    }
}
