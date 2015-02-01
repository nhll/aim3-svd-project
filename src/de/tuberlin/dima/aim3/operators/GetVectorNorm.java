package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.datatypes.VectorElement;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

public class GetVectorNorm extends RichGroupReduceFunction<Vector, VectorElement> {

    @Override
    public void reduce(Iterable<Vector> vectors, Collector<VectorElement> out) {
        int index = getRuntimeContext().<Integer>getBroadcastVariable("index").get(0);
        int norm = getRuntimeContext().<Integer>getBroadcastVariable("norm").get(0);
        vectors.forEach(vector -> out.collect(new VectorElement(index, vector.norm(norm))));
    }
}
