package de.tuberlin.dima.aim3.operators.extended;

import de.tuberlin.dima.aim3.Config;
import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.RichFilterFunction;

/**
 * Created by fsander on 08.02.15.
 */
public class FilterCurrentBeta extends RichFilterFunction<Element> {
    @Override
    public boolean filter(Element value) throws Exception {
        int stepNr = getIterationRuntimeContext().getSuperstepNumber();
        return value.getId() == Config.idOfTriag && value.getRow().equals(stepNr + 1) && value.getCol().equals(stepNr);
    }
}
