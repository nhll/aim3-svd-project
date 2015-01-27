package de.tuberlin.dima.aim3.operators;

import org.apache.flink.api.common.functions.FilterFunction;

public class RejectAll<T> implements FilterFunction<T> {

  @Override
  public boolean filter(T elem) {
    return false;
  }
}
