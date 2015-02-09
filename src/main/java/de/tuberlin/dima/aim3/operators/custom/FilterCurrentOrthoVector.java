package de.tuberlin.dima.aim3.operators.custom;

import de.tuberlin.dima.aim3.datatypes.Element;
import de.tuberlin.dima.aim3.operators.extended.FilterBasisVectorOfRelativeStep;
import org.apache.flink.api.java.DataSet;

/**
 * Created by fsander on 07.02.15.
 */
public class FilterCurrentOrthoVector extends AbstractCustomOperation<Element,Element> {

    @Override
    public DataSet createResult() {
        // because we did a pre step, the iteration counter is one behind
        return input.filter(new FilterBasisVectorOfRelativeStep(0));
    }
}
