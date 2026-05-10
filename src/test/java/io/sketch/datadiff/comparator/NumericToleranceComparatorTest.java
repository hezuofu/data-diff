package io.sketch.datadiff.comparator;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class NumericToleranceComparatorTest {

    @Test
    void testEqualsWithinTolerance() {
        NumericToleranceComparator comparator = new NumericToleranceComparator(0.01);
        assertTrue(comparator.equals(10.0, 10.005));
        assertTrue(comparator.equals(10.0, 9.995));
    }

    @Test
    void testEqualsOutsideTolerance() {
        NumericToleranceComparator comparator = new NumericToleranceComparator(0.01);
        assertFalse(comparator.equals(10.0, 10.02));
        assertFalse(comparator.equals(10.0, 9.98));
    }

    @Test
    void testEqualsWithNulls() {
        NumericToleranceComparator comparator = new NumericToleranceComparator(0.01);
        assertTrue(comparator.equals(null, null));
        assertFalse(comparator.equals(10.0, null));
        assertFalse(comparator.equals(null, 10.0));
    }

    @Test
    void testInvalidTolerance() {
        assertThrows(IllegalArgumentException.class, () -> new NumericToleranceComparator(-1.0));
    }
}
