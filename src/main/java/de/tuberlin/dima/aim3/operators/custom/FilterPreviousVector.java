package de.tuberlin.dima.aim3.operators.custom;

import de.tuberlin.dima.aim3.datatypes.Element;
import de.tuberlin.dima.aim3.operators.extended.FilterBasisVectorOfRelativeStep;
import org.apache.flink.api.java.DataSet;

/**
 * Filters out the previous Basis vector out of a workset. The result is v_{i-1}
 */
public class FilterPreviousVector extends AbstractCustomOperation<Element,Element> {

    @Override
    public DataSet createResult() {
        // superstep number equals id of previous basis vector, so 0 modification
        return input.filter(new FilterBasisVectorOfRelativeStep(0));
    }
}
