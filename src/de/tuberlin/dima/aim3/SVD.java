package de.tuberlin.dima.aim3;

import org.apache.flink.api.java.ExecutionEnvironment;

public class SVD {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // TODO: insert Lanczos data-flow here

    env.execute();

  }
}
