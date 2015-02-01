package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

public class VectorSubtraction extends RichGroupReduceFunction<Vector, Vector> {

    @Override
    public void reduce(Iterable<Vector> vectors, Collector<Vector> out) {
        Vector other = getRuntimeContext().<Vector>getBroadcastVariable("otherVector").get(0);
        vectors.forEach(vector -> out.collect(vector.minus(other)));
    }
}
