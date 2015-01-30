package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.util.Collector;

/**
 * A group reduce function for multiplying a set of vectors by a scalar value. Emits a new data set containing all the
 * vectors multiplied by the broadcasted scalar.
 */
public class VectorScalarMultiplication extends RichGroupReduceFunction<Vector, Vector> {

    /**
     * Takes an iterable collection of vectors and emits a new data set containing all the vectors multiplied by the
     * scalar value broadcasted as "scalar".
     *
     * @param vectors An iterable collection of vectors to multiply
     * @param out     A collector that all the resulting vectors will be added to
     */
    @Override
    public void reduce(Iterable<Vector> vectors, Collector<Vector> out) {
        double scalar = getRuntimeContext().<Double>getBroadcastVariable("scalar").get(0);
        for (Vector vector : vectors) {
            out.collect(vector.times(scalar));
        }
    }
}
