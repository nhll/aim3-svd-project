package de.tuberlin.dima.aim3.operators.custom;

import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;

/**
 * Sets the row(s) and col(s) of all Elements in the input
 */
public class SetRowAndCol extends AbstractCustomOperation<Element,Element> {

    private final Long row, col;

    public SetRowAndCol(Long row, Long col) {
        this.row = row;
        this.col = col;
    }

    @Override
    public DataSet<Element> createResult() {
        return input.map(new SetRowAndColMapper(row, col));
    }

    private static class SetRowAndColMapper implements MapFunction<Element,Element> {

        private final Long row, col;

        public SetRowAndColMapper(Long row, Long col) {
            this.row = row;
            this.col = col;
        }

        @Override
        public Element map(Element e) throws Exception {
            return new Element(e.getId(), row, col, e.getVal());
        }
    }
}
