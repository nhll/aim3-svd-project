package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class MatrixReader implements MapFunction<String, Vector> {

  @Override
  public Vector map(String row) {
    String[] tokens = row.split("\\s+");
    Integer rowIndex = new Integer(tokens[0]);
    ArrayList<Double> rowVec = new ArrayList<Double>();
    for (int i = 1; i < tokens.length; i++) {
      rowVec.add(i - 1, new Double(tokens[i]));
    }
    return new Vector(rowVec, rowIndex);
  }
}
