package de.tuberlin.dima.aim3.operators.extended;

import de.tuberlin.dima.aim3.Config;
import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Created by fsander on 08.02.15.
 */
public class FilterBasisVector implements FilterFunction<Element> {

    private final Long col;

    public FilterBasisVector(Long col) {
        this.col = col;
    }

    @Override
    public boolean filter(Element e) throws Exception {
        return e.getId() == Config.idOfBasis && e.getCol().equals(col);
    }
}
