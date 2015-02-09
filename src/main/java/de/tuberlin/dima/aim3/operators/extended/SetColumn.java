package de.tuberlin.dima.aim3.operators.extended;

import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by fsander on 08.02.15.
 */
public class SetColumn implements MapFunction<Element,Element> {

    private final Long col;

    public SetColumn(Long col) {
        this.col = col;
    }

    @Override
    public Element map(Element e) throws Exception {
        return new Element(e.getId(), e.getRow(), col, e.getVal());
    }
}
