package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.datatypes.VectorElement;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

public class DotProduct extends RichGroupReduceFunction<Vector, VectorElement> {

    @Override
    public void reduce(Iterable<Vector> vectors, Collector<VectorElement> out) throws IllegalArgumentException {
        Vector other = getRuntimeContext().<Vector>getBroadcastVariable("otherVector").get(0);
        vectors.forEach(vector -> out.collect(new VectorElement(vector.getIndex(), vector.dot(other))));
    }
}
