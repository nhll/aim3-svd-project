package de.tuberlin.dima.aim3.operators.extended;

import de.tuberlin.dima.aim3.Config;
import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Created by fsander on 09.02.15.
 */
public class BetaDuplicator implements FlatMapFunction<Element,Element> {
    @Override
    public void flatMap(Element e, Collector<Element> out) throws Exception {
        out.collect(e);
        if(e.getId() == Config.idOfTriag && !e.getCol().equals(e.getRow())) {
            out.collect(new Element(e.getId(), e.getCol(), e.getRow(), e.getVal()));
        }
    }
}
