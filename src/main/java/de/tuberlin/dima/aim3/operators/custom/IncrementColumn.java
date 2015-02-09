package de.tuberlin.dima.aim3.operators.custom;

import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Created by fsander on 07.02.15.
 */
public class IncrementColumn extends AbstractCustomOperation<Element,Element> {


    @Override
    public DataSet<Element> createResult() {
        return input.map(new IncrementColumnMapper());
    }

    private static class IncrementColumnMapper implements MapFunction<Element,Element> {
        @Override
        public Element map(Element e) throws Exception {
            return new Element(e.getId(), e.getRow(), e.getCol() + 1L, e.getVal());
        }
    }
}
