package de.tuberlin.dima.aim3.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class VectorElementsToSingleVector implements GroupReduceFunction<Tuple2<Integer, Double>,
                                                                         Tuple2<Integer, Double[]>> {

  private int vectorIndex;

  private VectorElementsToSingleVector() {
    // Default constructor is not allowed...
  }

  public VectorElementsToSingleVector(int vectorIndex) {
    this.vectorIndex = vectorIndex;
  }

  @Override
  public void reduce(Iterable<Tuple2<Integer, Double>> elements, Collector<Tuple2<Integer, Double[]>> out)
      throws IndexOutOfBoundsException {

    // First, store the elements in a hash map, mapping each value to its position (index) in the vector.
    HashMap<Integer, Double> values = new HashMap<Integer, Double>();
    for (Tuple2<Integer, Double> element : elements) {
      values.put(element.f0, element.f1);
    }

    // Now that we know how many values we have, create a double array representing the vector and store each value in
    // the set at the corresponding vector position.
    Double[] vector = new Double[values.size()];
    for (Map.Entry<Integer, Double> entry : values.entrySet()) {
      int valueIndex = entry.getKey();
      double value = entry.getValue();

      // Check if the index is actually valid. Safety first!
      if (valueIndex >= vector.length) {
        throw new IndexOutOfBoundsException("Value index " + valueIndex + " is out of bounds!");
      }

      vector[valueIndex] = value;
    }

    out.collect(new Tuple2<Integer, Double[]>(vectorIndex, vector));
  }
}
