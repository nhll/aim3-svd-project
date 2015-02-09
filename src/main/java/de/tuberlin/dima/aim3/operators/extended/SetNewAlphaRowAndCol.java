package de.tuberlin.dima.aim3.operators.extended;

import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by fsander on 08.02.15.
 */
public class SetNewAlphaRowAndCol implements MapFunction<Element,Element> {

    private final long step;

    public SetNewAlphaRowAndCol(long step) {
        this.step = step;
    }

    @Override
    public Element map(Element e) throws Exception {
        return new Element(e.getId(), step, step, e.getVal());
    }
}
