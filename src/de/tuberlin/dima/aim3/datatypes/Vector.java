package de.tuberlin.dima.aim3.datatypes;

import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;

public class Vector {

  /** This value is used for indices of vectors that are not part of a matrix and that thus have no indices. */
  public static final int NOINDEX = -1;

  private int index;
  private ArrayList<Double> values;

  public Vector (ArrayList<Double> values) {
    this(values, NOINDEX);
  }

  public Vector (ArrayList<Double> values, int index) {
    this.index = index;
    this.values = values;
  }

  /**
   * @return An index representing this vector's position in the matrix or Vector.NOINDEX if it's not part of a matrix
   */
  public int getIndex() {
    return index;
  }

  /**
   * @return The number of values that are contained in this vector
   */
  public int getSize() {
    return values.size();
  }

  /**
   * @return A String representing this vector
   */
  public String toString() {
    String[] valueStrings = new String[values.size()];
    for (int i = 0; i < values.size(); i++) {
      valueStrings[i] = values.get(i).toString();
    }
    return "[ " + StringUtils.join(", ", valueStrings) + " ]";
  }

}
