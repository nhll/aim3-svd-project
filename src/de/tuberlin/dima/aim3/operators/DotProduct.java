package de.tuberlin.dima.aim3.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class DotProduct extends RichGroupReduceFunction<Tuple2<Integer, Double[]>,
                                                        Tuple2<Integer, Double>> {

  @Override
  public void reduce(Iterable<Tuple2<Integer, Double[]>> tuples, Collector<Tuple2<Integer, Double>> out)
      throws IllegalArgumentException {

    Double result = 0.0;
    // TODO: Do we need the tuple's index information at this point? If not, get the ArrayList directly!
    Tuple2<Integer, Double[]> otherTuple =
        getRuntimeContext().<Tuple2<Integer, Double[]>>getBroadcastVariable("otherVector").get(0);
    Double[] otherVector = otherTuple.f1;
    int otherVecSize = otherVector.length;

    for (Tuple2<Integer, Double[]> tuple : tuples) {
      int rowIndex = tuple.f0;
      Double[] rowVector = tuple.f1;
      int rowVecSize = rowVector.length;

      // Throw an exception if the vectors are not the same size!
      if (rowVecSize != otherVecSize) {
        throw new IllegalArgumentException("Vector sizes don't match! (row vector: " + rowVecSize + ", " +
                                           "other vector: " + otherVecSize +  ")");
      }

      // Compute result = rowVector[0] * otherVector[0] + rowVector[1] * otherVector[1] + ...
      for (int i = 0; i < rowVecSize; i++) {
        result += rowVector[i] * otherVector[i];
      }

      out.collect(new Tuple2<Integer, Double>(rowIndex, result));
    }
  }
}
