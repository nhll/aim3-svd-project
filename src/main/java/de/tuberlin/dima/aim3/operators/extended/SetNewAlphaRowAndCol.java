package de.tuberlin.dima.aim3.operators.extended;

import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * Created by fsander on 08.02.15.
 */
public class SetNewAlphaRowAndCol extends RichMapFunction<Element,Element> {

    @Override
    public Element map(Element e) throws Exception {
        long superstepNumber = getIterationRuntimeContext().getSuperstepNumber();
        return new Element(e.getId(), superstepNumber + 1, superstepNumber + 1, e.getVal());
    }
}
