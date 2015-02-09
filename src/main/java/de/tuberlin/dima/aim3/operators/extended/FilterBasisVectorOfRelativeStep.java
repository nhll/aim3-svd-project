package de.tuberlin.dima.aim3.operators.extended;

import de.tuberlin.dima.aim3.Config;
import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.RichFilterFunction;

/**
 * Created by fsander on 08.02.15.
 */
public class FilterBasisVectorOfRelativeStep  extends RichFilterFunction<Element> {

    private final int superstepModifier;

    public FilterBasisVectorOfRelativeStep(int superstepModifier) {
        this.superstepModifier = superstepModifier;
    }

    @Override
    public boolean filter(Element e) throws Exception {
        long superstepNumber = getIterationRuntimeContext().getSuperstepNumber();
        return e.getCol().equals(superstepNumber + superstepModifier) && e.getId() == Config.idOfBasis;
    }
}