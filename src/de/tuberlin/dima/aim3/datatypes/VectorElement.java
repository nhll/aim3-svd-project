package de.tuberlin.dima.aim3.datatypes;

/**
 * This class can be used to represent a single vector element and exists mainly for convenience. It is simply a small
 * wrapper for the vector element's value as well as the position of that value in the corresponding vector in the form
 * of an index.
 */
public class VectorElement {

  private int index;
  private double value;

  /**
   * Constructs a new vector element with the provided index and value.
   *
   * @param index The element's position in the corresponding vector, starting from 0
   * @param value The element's value
   */
  public VectorElement(int index, double value) {
    this.index = index;
    this.value = value;
  }

  /**
   * Default constructor needed by Flink to be able to work with this class. Creates a new vector element with value 0.0
   * and index {@link Vector#NOINDEX}.
   */
  public VectorElement() {
    this(Vector.NOINDEX, 0.0);
  }

  /**
   * @return The element's index in the corresponding vector
   */
  public int getIndex() {
    return index;
  }

  /**
   * @param index This vector element's new index
   */
  public void setIndex(int index) {
    this.index = index;
  }

  /**
   * @return The element's value
   */
  public double getValue() {
    return value;
  }

  /**
   * @param value This vector element's new value
   */
  public void setValue(double value) {
    this.value = value;
  }

  /**
   * Generates a hash code that (hopefully) uniquely identifies this vector element. This method is internally used by
   * {@link java.util.HashSet} to ensure that no duplicate values exist in the same set. Without overloading this
   * method, working with hash sets of vector elements is not fun.
   *
   * @return A hash code that uniquely identifies this vector element
   */
  @Override
  public int hashCode() {
    return Integer.toString(index).concat(Double.toString(value)).hashCode();
  }

  /**
   * @return A string representation of this vector element
   */
  public String toString() {
    return String.format("(%d, %.4f)", index, value);
  }

}
