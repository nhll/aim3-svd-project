package de.tuberlin.dima.aim3.operators.custom;

import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Created by fsander on 08.02.15.
 */
public class CreateElementWithL2Norm extends AbstractCustomOperation<Element,Element> {

    private byte id;

    public CreateElementWithL2Norm(byte id) {
        this.id = id;
    }

    @Override
    public DataSet<Element> createResult() {
        return input.map(e -> e.getVal() * e.getVal()).reduce((x,y) -> x+y)
                .map(new SqrtWithIdMapper(id));
    }

    private static class SqrtWithIdMapper implements MapFunction<Double,Element> {

        private byte id;

        public SqrtWithIdMapper(byte id) {
            this.id = id;
        }

        @Override
        public Element map(Double v) throws Exception {
            return new Element((byte) id, -1L, -1L, Math.sqrt(v));
        }
    }



}
