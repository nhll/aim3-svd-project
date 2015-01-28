package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import org.apache.flink.api.common.functions.FilterFunction;

public class VectorIndexFilter implements FilterFunction<Vector> {

  private int index;

  public VectorIndexFilter(int index) {
    this.index = index;
  }

  @Override
  public boolean filter(Vector vector) throws Exception {
    return vector.getIndex() == index;
  }
}
