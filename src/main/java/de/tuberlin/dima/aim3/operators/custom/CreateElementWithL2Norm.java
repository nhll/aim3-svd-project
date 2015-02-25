package de.tuberlin.dima.aim3.operators.custom;

import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Treats the input data, as a single vector and calculates the L2 norm with all elements inside the dataset.
 * Make sure to filter the vector before applying this operator. The L2 norm is encapsulated into an Element with the
 * given id, and row and column equal to -1
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
            return new Element(id, -1L, -1L, Math.sqrt(v));
        }
    }



}
