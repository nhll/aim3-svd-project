package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Element;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.util.Collector;

/**
 * Created by fsander on 07.02.15.
 */
public class BinaryOperators {

    public static DataSet<Element> multiply(DataSet<Element> one, DataSet<Element> two, byte id) {
        return
                one.join(two).where(Element.COL).equalTo(Element.ROW).with(new MultiplyElements(id))
                        .groupBy(Element.COL, Element.ROW).reduceGroup(new CombineElements());
    }

    public static DataSet<Element> multiplySquared(DataSet<Element> one, DataSet<Element> two, byte id) {
        return
                one.join(two).where(Element.ROW).equalTo(Element.ROW).with(new MultiplyElementsTransposed(id))
                        .groupBy(Element.COL, Element.ROW).reduceGroup(new CombineElements());
    }

    public static DataSet<Element> dot(DataSet<Element> one, DataSet<Element> two, byte id) {
        return
                one.join(two).where(Element.ROW).equalTo(Element.ROW).with(new MultiplyElements(id))
                        .aggregate(Aggregations.SUM, Element.VAL);
    }

    public static DataSet<Element> scalarInverted(DataSet<Element> vector, DataSet<Element> scalar) {
        return vector.cross(scalar.map(e -> e.getVal())).with(new InvertedScalarCross());
    }

    public static DataSet<Element> scalar(DataSet<Element> vector, DataSet<Element> scalar) {
        return vector.cross(scalar.map(e -> e.getVal())).with(new ScalarCross());
    }

    public static DataSet<Element> scalarDouble(DataSet<Element> vector, DataSet<Double> scalar) {
        return vector.cross(scalar).with(new ScalarCross());
    }

    public static DataSet<Element> substract(DataSet<Element> one, DataSet<Element> two) {
        return one.join(two).where(Element.ROW).equalTo(Element.ROW).with(new SubstractJoin());
    }

    private static class MultiplyElements implements JoinFunction<Element, Element, Element> {

        byte id;

        public MultiplyElements(byte id) {
            this.id = id;
        }

        @Override
        public Element join(Element input, Element other) throws Exception {
            return new Element(id, input.getRow(), other.getCol(), input.getVal() * other.getVal());
        }
    }

    private static class MultiplyElementsTransposed implements JoinFunction<Element, Element, Element> {

        byte id;

        public MultiplyElementsTransposed(byte id) {
            this.id = id;
        }

        @Override
        public Element join(Element input, Element other) throws Exception {
            return new Element(id, input.getCol(), other.getCol(), input.getVal() * other.getVal());
        }
    }

    @RichGroupReduceFunction.Combinable
    private static class CombineElements extends RichGroupReduceFunction<Element, Element> {

        @Override
        public void reduce(Iterable<Element> values, Collector<Element> out) throws Exception {
            Byte id = null;
            Long row = null;
            Long col = null;
            Double val = 0.0;
            for (Element e : values) {
                if (id == null) {
                    id = e.getId();
                }
                if (!id.equals(e.getId())) {
                    throw new IllegalArgumentException("elements must have same id");
                }
                if (row == null) {
                    row = e.getRow();
                }
                if (!row.equals(e.getRow())) {
                    throw new IllegalArgumentException("elements must have same row");
                }
                if (col == null) {
                    col = e.getCol();
                }
                if (!col.equals(e.getCol())) {
                    throw new IllegalArgumentException("elements must have same col");
                }
                val += e.getVal();
            }
            // maintain sparsity
            if (val != 0.0) {
                out.collect(new Element(id, row, col, val));
            }
        }

        @Override
        public void combine(Iterable<Element> values, Collector<Element> out) throws Exception {
            this.reduce(values, out);
        }
    }

    private static class ScalarCross implements CrossFunction<Element, Double, Element> {
        @Override
        public Element cross(Element val1, Double scalar) throws Exception {
            return new Element(val1.getId(), val1.getRow(), val1.getCol(), val1.getVal() * scalar);
        }
    }

    private static class InvertedScalarCross implements CrossFunction<Element, Double, Element> {
        @Override
        public Element cross(Element val1, Double scalar) throws Exception {
            return new Element(val1.getId(), val1.getRow(), val1.getCol(), val1.getVal() * (1.0 / scalar));
        }
    }

    private static class SubstractJoin implements JoinFunction<Element, Element, Element> {

        @Override
        public Element join(Element one, Element two) throws Exception {
            return new Element(one.getId(), one.getRow(), one.getCol(), one.getVal() - two.getVal());
        }
    }

}
