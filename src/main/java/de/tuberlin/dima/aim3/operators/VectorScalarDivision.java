package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.datatypes.VectorElement;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

public class VectorScalarDivision extends RichGroupReduceFunction<Vector, Vector> {

    @Override
    public void reduce(Iterable<Vector> vectors, Collector<Vector> out) {
        double scalar = getRuntimeContext().<VectorElement>getBroadcastVariable("scalar").get(0).getValue();
        vectors.forEach(vector -> {
            Vector result = vector.divideBy(scalar);
            System.out.println("VECTOR INDEX: " + vector.getIndex());
            // TODO: Make the incremented index optional!
            result.setIndex(result.getIndex() + 1);
            out.collect(result);
        });
    }
}
