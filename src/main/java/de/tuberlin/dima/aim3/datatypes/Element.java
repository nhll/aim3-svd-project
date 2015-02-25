package de.tuberlin.dima.aim3.datatypes;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Wrapper class over Tuple4 with getters
 */
public class Element extends Tuple4<Byte, Long, Long, Double> {

    public Element() {}

    public Element(Byte id, Long row, Long col, Double val) {
        super(id, row, col, val);
    }

    /*
        indices
     */

    public static final int ID = 0;

    public static final int ROW = 1;

    public static final int COL = 2;

    public static final int VAL = 3;

    /*
        getters
     */

    public byte getId() {
        return f0;
    }

    public Long getRow() {
        return f1;
    }

    public Long getCol() {
        return f2;
    }

    public Double getVal() {
        return f3;
    }

}
