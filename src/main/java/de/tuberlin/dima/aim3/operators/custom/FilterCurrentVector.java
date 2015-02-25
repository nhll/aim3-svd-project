package de.tuberlin.dima.aim3.operators.custom;

import de.tuberlin.dima.aim3.datatypes.Element;
import de.tuberlin.dima.aim3.operators.extended.FilterBasisVectorOfRelativeStep;
import org.apache.flink.api.java.DataSet;

/**
 * Filters out the current Basis vector out of a workset. The result is v_i
 */
public class FilterCurrentVector extends AbstractCustomOperation<Element,Element> {

    @Override
    public DataSet createResult() {
        // superstepnumber is one behind actual iteration, so modify it +1 to get current vector
        return input.filter(new FilterBasisVectorOfRelativeStep(1));
    }
}
