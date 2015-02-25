package de.tuberlin.dima.aim3.operators.extended;

import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * Sets the proper row and col for a newly beta alpha. That is for the row: superstep numner + 2; and for the col: superstep numner + 1
 */
public class SetNewBetaRowAndCol extends RichMapFunction<Element,Element> {

    @Override
    public Element map(Element e) throws Exception {
        long superstepNumber = getIterationRuntimeContext().getSuperstepNumber();
        return new Element(e.getId(), superstepNumber + 2, superstepNumber + 1, e.getVal());
    }
}
