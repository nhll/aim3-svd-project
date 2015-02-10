package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.datatypes.VectorElement;
import org.apache.flink.api.common.functions.JoinFunction;

import java.util.List;

public class BetaExtender implements JoinFunction<Vector, VectorElement, Vector> {
    @Override
    public Vector join(Vector vector, VectorElement b) throws Exception {
        List<Double> elements = vector.getElements();
        int index = vector.getIndex();
        if (index + 1 < elements.size()) {
            elements.set(index + 1, b.getValue());
        }
        return new Vector(elements, index);
    }
}
