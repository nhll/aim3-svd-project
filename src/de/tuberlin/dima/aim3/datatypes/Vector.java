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
   * Constructs and returns a new zero vector of type U (all elements set to 0) that is part of a matrix and thus has an
   * index. Only supports types that an Integer value can be casted to, e.g. Double, Long, ...
   *
   * @param size  The zero vector's number of elements
   * @param index The zero vector's index in the corresponding matrix
   * @param clazz The Class of the zero vector's type U (e.g. Double.class for a zero vector of type Double)
   * @param <U>   The zero vector's type (the type of all its elements, e.g. Double)
   * @return A new vector of type U, of the specified size and with the specified index, with all elements set to 0
   */
  public static <U> Vector<U> getZeroVector(int size, int index, Class<U> clazz) {
    ArrayList<U> elements = new ArrayList<U>();
    for (int i = 0; i < size; i++) {
      elements.add(i, clazz.cast(0));
    }
    return new Vector<U>(elements, index);
  }

  /**
   * Same as {@link #getZeroVector(int, int, Class)}, but for zero vectors that are not part of a matrix and thus do not
   * have an index. The resulting zero vector's index will be set to Vector.NOINDEX.
   *
   * @param size  The zero vector's number of elements
   * @param clazz The Class of the zero vector's type U (e.g. Double.class for a zero vector of type Double)
   * @param <U>   The zero vector's type (the type of all its elements, e.g. Double)
   * @return A new vector of type U and of the specified size with all elements set to 0
   */
  public static <U> Vector<U> getZeroVector(int size, Class<U> clazz) {
    return getZeroVector(size, NOINDEX, clazz);
  }

  /**
   * Constructs a vector that's part of a matrix from a list of values that should be the vector's elements. The vector
   * being part of a matrix means that it will have an index value representing its position in the matrix.
   *
   * @param elements A list of the vector's values
   * @param index    The vector's row or column index in the matrix
   */
  public Vector (List<T> elements, int index) {
    this.index = index;
    this.elements = elements;
  }

  /**
   * Same as {@link #Vector(java.util.List, int)}, but for constructing a vector that's not part of a matrix and thus
   * does not have an index value. Using this constructor, the vector's index will be set to
   * {@link #NOINDEX Vector.NOINDEX}.
   *
   * @param elements A list of the vector's values
   */
  public Vector (List<T> elements) {
    this(elements, NOINDEX);
  }

  /**
   * Same as {@link #Vector(java.util.List, int)}, but takes a set of VectorElement instances instead of a list of
   * values.
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
   * Same as {@link #Vector(java.util.Set)}, but for constructing a vector that's not part of a matrix and thus does
   * not have an index value. Using this constructor, the vector's index will be set to {@link #NOINDEX Vector.NOINDEX}.
   *
   * @param elements A set of VectorElement objects representing the vector's values
   */
  public Vector (Set<VectorElement<T>> elements) {
    this(elements, NOINDEX);
  }

  /**
   * @return An index representing this vector's position in the matrix or Vector.NOINDEX if it's not part of a matrix
   */
  public int getIndex() {
    return index;
  }

  /**
   * @param index The value that this vector's index should be set to
   * @return A reference to this vector in order to allow for method chaining
   */
  public Vector<T> setIndex(int index) {
    this.index = index;
    return this;
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
