package de.tuberlin.dima.aim3.datatypes;

import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

// TODO: Implement vector algebra
// TODO: Class documentation comment
public class Vector<T> {

  /** This value is used for indices of vectors that are not part of a matrix and that thus have no indices. */
  public static final int NOINDEX = -1;

  private int index;
  private List<T> elements;

  /**
   * Constructs a vector that's not part of a matrix from a list of values that the vector should contain. Using this
   * constructor, the vector's index will be set to Vector.NOINDEX.
   *
   * @param elements A list of the vector's values
   */
  public Vector (List<T> elements) {
    this(elements, NOINDEX);
  }

  /**
   * Same as {@link #Vector(java.util.List)}, but for constructing a vector that's part of a matrix and thus has a row
   * or column index.
   *
   * @param elements A list of the vector's values
   * @param index    The vector's row or column index in the matrix
   */
  public Vector (List<T> elements, int index) {
    this.index = index;
    this.elements = elements;
  }

  /**
   * Same as {@link #Vector(java.util.List)}, but takes a set of VectorElement objects instead of a list of values.
   *
   * @param elements A set of VectorElement objects representing the vector's values
   */
  public Vector (Set<VectorElement<T>> elements) {
    this(elements, NOINDEX);
  }

  /**
   * Same as {@link #Vector(java.util.Set)}, but for constructing a vector that's part of a matrix and thus has a row or
   * column index.
   *
   * @param elements A set of VectorElement objects representing the vector's values
   * @param index    The vector's row or column index in the matrix
   */
  public Vector (Set<VectorElement<T>> elements, int index) {
    this.index = index;
    this.elements = new ArrayList<T>(elements.size());
    for (VectorElement<T> element : elements) {
      this.elements.add(element.getIndex(), element.getValue());
    }
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
   * @return {@code true} if this vector is part of a matrix, {@code false} otherwise
   */
  public boolean isMatrixVector() {
    return index != NOINDEX;
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
