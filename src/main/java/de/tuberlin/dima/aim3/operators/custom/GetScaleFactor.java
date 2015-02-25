package de.tuberlin.dima.aim3.operators.custom;

import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.java.DataSet;

/**
 * Calculates the current scale factor. See Mahout LanczosSolver.
 */
public class GetScaleFactor extends AbstractCustomOperation<Element,Double> {
    @Override
    public DataSet<Double> createResult() {
        return input.map(e -> e.getVal() * e.getVal()).reduce((x, y) -> x + y).map(x -> 1.0 / Math.sqrt(x));
    }
}