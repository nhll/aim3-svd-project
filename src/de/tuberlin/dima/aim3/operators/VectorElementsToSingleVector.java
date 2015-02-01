package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Vector;
import de.tuberlin.dima.aim3.datatypes.VectorElement;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class VectorElementsToSingleVector implements GroupReduceFunction<VectorElement, Vector> {

    private int vectorIndex;

    public VectorElementsToSingleVector(int vectorIndex) {
        this.vectorIndex = vectorIndex;
    }

    public VectorElementsToSingleVector() {
        this(Vector.NOINDEX);
    }

    @Override
    public void reduce(Iterable<VectorElement> elements, Collector<Vector> out) {
        // Store all index/value pairs in a map and then emit a new vector created from that map.
        HashMap<Integer, Double> elementMap = new HashMap<>();
        elements.forEach(element -> elementMap.put(element.getIndex(), element.getValue()));
        out.collect(new Vector(elementMap, vectorIndex));
    }
}
