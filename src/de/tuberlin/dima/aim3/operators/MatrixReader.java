package de.tuberlin.dima.aim3.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MatrixReader implements MapFunction<String, Tuple2<Long, Double[]>> {
  @Override
  public Tuple2<Long, Double[]> map(String row) {
    String[] tokens = row.split("\\s+");
    Long rowIndex = new Long(tokens[0]);
    Double[] rowVec = new Double[tokens.length - 1];
    for (int i = 1; i < tokens.length; i++) {
      rowVec[i] = new Double(tokens[i]);
    }
    return new Tuple2<Long, Double[]>(rowIndex, rowVec);
  }
}
