package de.tuberlin.dima.aim3.operators;

import de.tuberlin.dima.aim3.datatypes.Indexed;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Filter function that allows filtering a data set of indexed elements so that the resulting data set only contains
 * elements whose indices match the index specified during IndexFilter construction.
 *
 * @param <T> The type of the elements in the data set that should be filtered; has to implement {@link Indexed}
 */
public class IndexFilter<T extends Indexed> implements FilterFunction<T> {

    private int index;

    /**
     * @param index The index value that should be used for filtering; only elements with this index will be contained
     *              in the filtered data set
     */
    public IndexFilter(int index) {
        this.index = index;
    }

    /**
     * Decides for a single element of type {@link T} whether or not it passes the filter.
     *
     * @param element The element that should be filtered
     * @return {@code true} if the element passes the filter, {@code false} if it doesn't
     */
    @Override
    public boolean filter(T element) {
        return element.getIndex() == index;
    }
}
