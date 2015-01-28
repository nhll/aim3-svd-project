package de.tuberlin.dima.aim3.datatypes;

import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

// TODO: Implement vector algebra
// TODO: Class documentation comment
public class Vector {

  /** This value is used for indices of vectors that are not part of a matrix and that thus have no indices. */
  public static final int NOINDEX = -1;

  private int index;
  private List<Double> elements;

  /**
   * Constructs and returns a new zero vector (all elements set to 0) that is part of a matrix and thus has an index.
   *
   * @param size  The zero vector's number of elements
   * @param index The zero vector's index in the corresponding matrix
   * @return A new vector of the specified size and with the specified index, with all elements set to 0
   */
  public static Vector getZeroVector(int size, int index) {
    ArrayList<Double> elements = new ArrayList<Double>();
    for (int i = 0; i < size; i++) {
      elements.add(i, 0.0);
    }
    return new Vector(elements, index);
  }

  /**
   * Same as {@link #getZeroVector(int, int)}, but for zero vectors that are not part of a matrix and thus do not
   * have an index. The resulting zero vector's index will be set to Vector.NOINDEX.
   *
   * @param size  The zero vector's number of elements
   * @return A new vector of type U and of the specified size with all elements set to 0
   */
  public static Vector getZeroVector(int size) {
    return getZeroVector(size, NOINDEX);
  }

  /**
   * Generates a new random vector of the specified size (being the number of elements in the vector) and with the
   * specified L2-norm and row/column index. It does this by generating a vector of the specified size filled with
   * random values between 0.0 and 1.0 and then scaling that vector to the specified target norm.
   *
   * @see #scaleTo(double)
   *
   * @param size  The number of elements that the generated vector should have
   * @param norm  The L2-norm that the generated vector should have
   * @param index The row/column index of the generated vector in the corresponding matrix
   * @return A new random vector of the specified size and with the specified L2-norm and row/column index
   */
  public static Vector getRandomVector(int size, double norm, int index) {
    ArrayList<Double> elements = new ArrayList<Double>();
    Random r = new Random();
    for (int i = 0; i < size; i++) {
      elements.add(i, r.nextDouble());
    }
    return new Vector(elements, index).scaleTo(norm);
  }

  /**
   * Same as {@link #getRandomVector(int, double, int)}, but for creating a random vector that does not belong to a
   * matrix and thus does not have a row/column index.
   *
   * @see #getRandomVector(int, double, int)
   * @see #scaleTo(double)
   *
   * @param size The number of elements that the generated vector should have
   * @param norm The L2-norm that the generated vector should have
   * @return A new random vector of the specified size and with the specified L2-norm
   */
  public static Vector getRandomVector(int size, double norm) {
    return getRandomVector(size, norm, NOINDEX);
  }

  /**
   * Constructs a vector that's part of a matrix from a list of values that should be the vector's elements. The vector
   * being part of a matrix means that it will have an index value representing its position in the matrix.
   *
   * @param elements A list of the vector's values
   * @param index    The vector's row or column index in the matrix
   */
  public Vector (List<Double> elements, int index) {
    this.index = index;
    this.elements = elements;
  }

  /**
   * Same as {@link #Vector(java.util.List, int)}, but for constructing a vector that's not part of a matrix and thus
   * does not have an index value. Using this constructor, the vector's index will be set to
   * {@link #NOINDEX Vector.NOINDEX}.
   *
   * @see #Vector(java.util.List, int)
   *
   * @param elements A list of the vector's values
   */
  public Vector (List<Double> elements) {
    this(elements, NOINDEX);
  }

  /**
   * Same as {@link #Vector(java.util.List, int)}, but takes a set of VectorElement instances instead of a list of
   * values.
   *
   * @see #Vector(java.util.List, int)
   *
   * @param elements A set of VectorElement objects representing the vector's values
   * @param index    The vector's row or column index in the matrix
   */
  public Vector (Set<VectorElement> elements, int index) {
    this.index = index;

    // For safety purposes, create a new list with as many 0.0 values as there are vector elements in the set. This way,
    // the order in which the elements in the set get inserted into the list doesn't matter and we're not running the
    // risk of producing an IndexOutOfBoundsException. Of course this can still happen if the indices are corrupted.
    ArrayList<Double> elementList = new ArrayList<Double>();
    for (int i = 0; i < elements.size(); i++) {
      elementList.add(i, 0.0);
    }

    // Insert the set's vector elements into the list, making sure that all the indices are actually valid.
    for (VectorElement element : elements) {
      int elementIndex = element.getIndex();
      if (elementIndex < elementList.size()) {
        elementList.set(elementIndex, element.getValue());
      } else {
        throw new IndexOutOfBoundsException("Index: " + elementIndex + ", Size: " + elementList.size());
      }
    }
    this.elements = elementList;
  }

  /**
   * Same as {@link #Vector(java.util.Set, int)}, but for constructing a vector that's not part of a matrix and thus
   * does not have an index value. Using this constructor, the vector's index will be set to
   * {@link #NOINDEX Vector.NOINDEX}.
   *
   * @see #Vector(java.util.Set, int)
   *
   * @param elements A set of VectorElement objects representing the vector's values
   */
  public Vector (Set<VectorElement> elements) {
    this(elements, NOINDEX);
  }

  /**
   * Copy constructor for creating a new vector as a copy of the provided vector.
   *
   * @param other The vector that should be copied to the new vector
   */
  public Vector (Vector other) {
    index = other.index;
    elements = other.elements;
  }

  /**
   * Default constructor needed by Flink to be able to work with this class. Constructs an empty vector with the index
   * {@link #NOINDEX Vector.NOINDEX}.
   */
  public Vector() {
    this(new ArrayList<Double>(), NOINDEX);
  }

  /**
   * @return An index representing this vector's position in the matrix or Vector.NOINDEX if it's not part of a matrix
   */
  public int getIndex() {
    return index;
  }

  /**
   * @param index The value that should be used as this vector's index
   */
  public void setIndex(int index) {
    this.index = index;
  }

  /**
   * @return A list containing this vector's elements
   */
  public List<Double> getElements() {
    return elements;
  }

  /**
   * @param elements A list of values that should be used as this vector's elements
   */
  public void setElements(List<Double> elements) {
    this.elements = elements;
  }

  /**
   * @param index The index of the vector element that should be returned
   * @return This vector's element at the provided index
   */
  public double get(int index) {
    return elements.get(index);
  }

  /**
   * @return The number of elements that are contained in this vector
   */
  public int size() {
    return elements.size();
  }

  /**
   * Calculates the p-norm for this vector. The p-norm of vector x with the elements x1, x2, ..., xn is defined as
   * |x|p = (x1^p + x2^p + ... + xn^p)^(1/p).
   *
   * @param p Specifies which norm should be calculated
   * @return The p-norm of this vector
   */
  public double norm(int p) {
    double sumOfPowers = 0;
    for (double element : elements) {
      sumOfPowers += Math.pow(element, p);
    }
    return Math.pow(sumOfPowers, 1 / p);
  }

  /**
   * Convenience method for calculating this vector's L2-norm without having to explicitly pass p = 2 to
   * {@link #norm(int)}.
   *
   * @return The L2-norm of this vector
   */
  public double norm() {
    return norm(2);
  }

  /**
   * Scales this vector to the specified target norm by dividing it by its current norm and multiplying the resulting
   * vector by the target norm. Does not alter this vector, but instead returns a new vector that is equal to this
   * vector scaled to the provided target norm.
   *
   * If the provided target norm equals {@link Double#NaN}, an exact copy of this vector will be returned.
   *
   * @param norm The target norm to scale this vector to
   * @return A new vector resulting from the scaling of this vector to the provided target norm
   */
  public Vector scaleTo(double norm) {
    return norm == Double.NaN ? new Vector(this) : this.divideBy(this.norm()).times(norm);
  }

  /**
   * Calculates the dot product of this vector and another vector. The dot product of two vectors is the sum of the
   * products of the corresponding elements of the two vectors. It is required that both vectors are equal in size.
   *
   * @param other The other vector for the dot product computation
   * @return The dot product of this vector and the other vector
   * @throws IllegalArgumentException if the two vectors are not equal in size
   */
  public double dot(Vector other) throws IllegalArgumentException {
    int size = size();
    if (size != other.size()) {
      throw new IllegalArgumentException("Vector sizes don't match!");
    }

    double result = 0.0;
    for (int i = 0; i < size; i++) {
      result += get(i) * other.get(i);
    }
    return result;
  }

  /**
   * Multiplies this vector by x by multiplying each element by x. Does not alter this vector, but instead returns a new
   * vector that is equal to this vector multiplied by x.
   *
   * @param x The scalar to multiply this vector by
   * @return A new vector resulting from the multiplication of this vector by x
   */
  public Vector times(double x) {
    ArrayList<Double> multipliedElements = new ArrayList<Double>();
    for (Double element : elements) {
      multipliedElements.add(element * x);
    }
    return new Vector(multipliedElements, index);
  }

  /**
   * Divides this vector by x by dividing each element by x. Does not alter this vector, but instead returns a new
   * vector that is equal to this vector divided by x.
   *
   * @param x The scalar to divide this vector by
   * @return A new vector resulting from the division of this vector by x
   */
  public Vector divideBy(double x) {
    ArrayList<Double> dividedElements = new ArrayList<Double>();
    for (Double element : elements) {
      dividedElements.add(element / x);
    }
    return new Vector(dividedElements, index);
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
