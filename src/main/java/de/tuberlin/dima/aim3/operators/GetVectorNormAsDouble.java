package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

public class GetVectorNormAsDouble extends RichGroupReduceFunction<Vector, Double> {
    @Override
    public void reduce(Iterable<Vector> vectors, Collector<Double> out) throws Exception {
        int norm = getRuntimeContext().<Integer>getBroadcastVariable("norm").get(0);
        vectors.forEach(vector -> {
            System.out.println("---> " + vector.norm(norm) + " (vector: (" + vector + "))");
            out.collect(vector.norm(norm));
        });
    }
}
