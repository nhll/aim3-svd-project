package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.datatypes.VectorElement;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class VectorElementsToSingleVector implements GroupReduceFunction<VectorElement, Vector> {

  private int vectorIndex;

  public VectorElementsToSingleVector(int vectorIndex) {
    this.vectorIndex = vectorIndex;
  }

  public VectorElementsToSingleVector() {
    this(Vector.NOINDEX);
  }

  @Override
  public void reduce(Iterable<VectorElement> elements, Collector<Vector> out) {
    // Store all vector elements in a set and then emit a new vector created from that set.
    HashSet<VectorElement> elementSet = new HashSet<VectorElement>();
    for (VectorElement element : elements) {
      if (elementSet.add(element)) {
        System.out.println("Element " + element + " added to set!");
      } else {
        System.out.println("Element " + element + " already contained in set!");
      }
    }
    out.collect(new Vector(elementSet, vectorIndex));
  }
}
