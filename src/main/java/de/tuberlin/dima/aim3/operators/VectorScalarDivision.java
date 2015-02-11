package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.datatypes.VectorElement;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

public class VectorScalarDivision extends RichGroupReduceFunction<Vector, Vector> {

    private boolean incrementIndex;

    public VectorScalarDivision(boolean incrementIndex) {
        this.incrementIndex = incrementIndex;
    }

    public VectorScalarDivision() {
        this(false);
    }

    @Override
    public void reduce(Iterable<Vector> vectors, Collector<Vector> out) {
        double scalar;
        try {
            scalar = getRuntimeContext().<VectorElement>getBroadcastVariable("scalar").get(0).getValue();
        } catch (ClassCastException e) {
            scalar = getRuntimeContext().<Double>getBroadcastVariable("scalar").get(0);
        }
        final double scalarFinal = scalar;
        vectors.forEach(vector -> {
            Vector result = vector.divideBy(scalarFinal);
            if (incrementIndex) {
                result.setIndex(result.getIndex() + 1);
            }
//            System.out.println("(" + vector + ") / " + scalarFinal + " --> (" + result + ")");
            out.collect(result);
        });
    }
}
