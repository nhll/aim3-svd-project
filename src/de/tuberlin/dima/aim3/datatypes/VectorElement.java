package de.tuberlin.dima.aim3.datatypes;

/**
 * This class can be used to represent a single vector element and exists mainly for convenience. It is simply a small
 * wrapper for the vector element's value as well as the position of that value in the corresponding vector in the form
 * of an index.
 */
public class VectorElement<T> {

  private int index;
  private T value;

  /**
   * Constructs a new vector element with the provided index and value.
   *
   * @param index The element's position in the corresponding vector, starting from 0
   * @param value The element's value
   */
  public VectorElement(int index, T value) {
    this.index = index;
    this.value = value;
  }

  /**
   * @return The element's index in the corresponding vector
   */
  public int getIndex() {
    return index;
  }

  /**
   * @return The element's value
   */
  public T getValue() {
    return value;
  }

  /**
   * Checks this vector element for equality to another vector element. Two vector elements are equal if their indices
   * and their values are the same. This method is needed to ensure that sets of vector elements can't contain the same
   * vector element more than once.
   *
   * @param other The other vector element to compare this one to
   * @return {@code true} if both vector elements are equal, {@code false} otherwise
   */
  public boolean equals(VectorElement<T> other) {
    return index == other.index &&
           value == other.value;
  }

}
