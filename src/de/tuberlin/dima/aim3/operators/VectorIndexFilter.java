package de.tuberlin.dima.aim3.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class VectorIndexFilter implements FilterFunction<Tuple2<Integer, Double[]>> {

  private int index;

  private VectorIndexFilter() {
    // Default constructor not allowed, because we always need an index.
  }

  public VectorIndexFilter(int index) {
    this.index = index;
  }

  @Override
  public boolean filter(Tuple2<Integer, Double[]> vector) throws Exception {
    return vector.f0 == index;
  }
}
