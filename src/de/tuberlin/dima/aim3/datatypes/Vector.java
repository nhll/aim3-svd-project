package de.tuberlin.dima.aim3.datatypes;

import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;

public class Vector {

  /** This value is used for indices of vectors that are not part of a matrix and that thus have no indices. */
  public static final int NOINDEX = -1;

  private int index;
  private ArrayList<Double> elements;

  public Vector (ArrayList<Double> elements) {
    this(elements, NOINDEX);
  }

  public Vector (ArrayList<Double> elements, int index) {
    this.index = index;
    this.elements = elements;
  }

  /**
   * @return An index representing this vector's position in the matrix or Vector.NOINDEX if it's not part of a matrix
   */
  public int getIndex() {
    return index;
  }

  /**
   * @return The number of elements that are contained in this vector
   */
  public int getSize() {
    return elements.size();
  }

  /**
   * @return A String representing this vector
   */
  public String toString() {
    String[] valueStrings = new String[elements.size()];
    for (int i = 0; i < elements.size(); i++) {
      valueStrings[i] = elements.get(i).toString();
    }
    return "[ " + StringUtils.join(", ", valueStrings) + " ]";
  }

}
