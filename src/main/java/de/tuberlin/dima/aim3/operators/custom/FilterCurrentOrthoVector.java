package de.tuberlin.dima.aim3.operators.custom;

import de.tuberlin.dima.aim3.datatypes.Element;
import de.tuberlin.dima.aim3.operators.extended.FilterBasisVectorOfRelativeStep;
import org.apache.flink.api.java.DataSet;

/**
 * Filters the current Basis vector out of the orthogonalization input.
 */
public class FilterCurrentOrthoVector extends AbstractCustomOperation<Element,Element> {

    @Override
    public DataSet createResult() {
        return input.filter(new FilterBasisVectorOfRelativeStep(0));
    }
}
