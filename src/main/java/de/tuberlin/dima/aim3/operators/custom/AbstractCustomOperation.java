package de.tuberlin.dima.aim3.operators.custom;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;

/**
 * Created by fsander on 07.02.15.
 */
public abstract class AbstractCustomOperation<IN, OUT> implements CustomUnaryOperation<IN, OUT> {

    protected DataSet<IN> input;

    @Override
    public void setInput(DataSet<IN> inputData) {
        input = inputData;
    }

}
