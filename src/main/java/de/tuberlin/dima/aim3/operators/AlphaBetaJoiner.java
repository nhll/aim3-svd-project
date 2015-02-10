package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.datatypes.VectorElement;
import org.apache.flink.api.common.functions.RichJoinFunction;

import java.util.ArrayList;
import java.util.List;

public class AlphaBetaJoiner extends RichJoinFunction<VectorElement, VectorElement, Vector> {
    @Override
    public Vector join(VectorElement a, VectorElement b) throws Exception {
        int colCount = getRuntimeContext().<Integer>getBroadcastVariable("colCount").get(0);
        List<Double> vectorElements = new ArrayList<>();
        int index = a.getIndex();

        // Fill the vector with as many 0s as we have columns.
        for (int i = 0; i < colCount; i++) {
            vectorElements.add(0.0);
        }

        if (index > 0) {
            vectorElements.set(index - 1, a.getValue());
            if (index > 1) {
                vectorElements.set(index - 2, b.getValue());
            }
        } else {
            throw new IndexOutOfBoundsException("INDEX FAIL! (index: " + index + ")");
        }

        return new Vector(vectorElements, index - 1);
    }
}
