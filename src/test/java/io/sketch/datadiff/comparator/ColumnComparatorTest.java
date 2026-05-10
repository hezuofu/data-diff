package io.sketch.datadiff.comparator;

import io.sketch.datadiff.core.model.CompareOptions;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class ColumnComparatorTest {

    @Test
    void testNulls() {
        ColumnComparator comp = new ColumnComparator(CompareOptions.defaults());
        assertTrue(comp.equals(null, null, "col"));
        assertFalse(comp.equals("a", null, "col"));
        assertFalse(comp.equals(null, "b", "col"));
    }

    @Test
    void testExcludedColumn() {
        CompareOptions opts = CompareOptions.builder()
            .excludeColumns("ignored")
            .build();
        ColumnComparator comp = new ColumnComparator(opts);
        assertTrue(comp.equals("different", "values", "ignored"));
    }

    @Test
    void testCaseInsensitiveGlobal() {
        CompareOptions opts = CompareOptions.builder().ignoreCase(true).build();
        ColumnComparator comp = new ColumnComparator(opts);
        assertTrue(comp.equals("Hello", "hello", "name"));
    }

    @Test
    void testCaseInsensitivePerColumn() {
        CompareOptions opts = CompareOptions.builder()
            .caseInsensitiveColumns(Set.of("comment"))
            .build();
        ColumnComparator comp = new ColumnComparator(opts);
        assertTrue(comp.equals("Repeat", "repeat", "comment"));
        assertFalse(comp.equals("Repeat", "repeat", "name"));
    }

    @Test
    void testNumericTolerance() {
        CompareOptions opts = CompareOptions.builder().numericTolerance(0.01).build();
        ColumnComparator comp = new ColumnComparator(opts);
        assertTrue(comp.equals(10.00, 10.005, "price"));
        assertFalse(comp.equals(10.00, 10.02, "price"));
    }

    @Test
    void testNumericExactWhenNoTolerance() {
        ColumnComparator comp = new ColumnComparator(CompareOptions.defaults());
        assertTrue(comp.equals(10.0, 10.0, "qty"));
        assertFalse(comp.equals(10.0, 10.001, "qty"));
    }

    @Test
    void testDefaultEquality() {
        ColumnComparator comp = new ColumnComparator(CompareOptions.defaults());
        assertTrue(comp.equals("hello", "hello", "name"));
        assertFalse(comp.equals("hello", "world", "name"));
        assertTrue(comp.equals(42, 42, "id"));
    }
}
