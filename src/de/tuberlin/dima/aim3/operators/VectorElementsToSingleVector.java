package de.tuberlin.dima.aim3.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

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
  public void reduce(Iterable<Tuple2<Integer, Double>> elements, Collector<Tuple2<Integer, Double[]>> out) {
    ArrayList<Double> vector = new ArrayList<Double>();
    out.collect(new Tuple2<Integer, Double[]>(vectorIndex, vector.toArray(new Double[0])));
  }
}
