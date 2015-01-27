package de.tuberlin.dima.aim3.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class MatrixReader implements MapFunction<String, Tuple2<Integer, Double[]>> {
  @Override
  public Tuple2<Integer, Double[]> map(String row) {
    String[] tokens = row.split("\\s+");
    Integer rowIndex = new Integer(tokens[0]);
    Double[] rowVec = new Double[tokens.length - 1];
    // TODO: Is an integer index enough?
    for (int i = 0; i < tokens.length; i++) {
      rowVec[i] = new Double(tokens[i]);
    }
    return new Tuple2<Integer, Double[]>(rowIndex, rowVec);
  }
}
