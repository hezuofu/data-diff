package io.sketch.datadiff.comparator;

import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.core.model.ColumnDef;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CompositeComparatorTest {

    private final CompositeComparator comparator = new CompositeComparator(CompareOptions.defaults());

    @Test
    void testCompareNumericEqual() {
        ColumnDef col = new ColumnDef("amount", "DECIMAL(10,2)", false);
        assertTrue(comparator.equals(100.0, 100.0, col));
        assertTrue(comparator.equals(0.0, 0.00009, col));
    }

    @Test
    void testCompareNumericDifferent() {
        ColumnDef col = new ColumnDef("amount", "DECIMAL(10,2)", false);
        assertFalse(comparator.equals(100.0, 101.0, col));
    }

    @Test
    void testCompareNumericNulls() {
        ColumnDef col = new ColumnDef("amount", "DECIMAL(10,2)", false);
        assertTrue(comparator.equals(null, null, col));
        assertFalse(comparator.equals(1.0, null, col));
        assertFalse(comparator.equals(null, 1.0, col));
    }

    @Test
    void testCompareStringEqual() {
        ColumnDef col = new ColumnDef("name", "VARCHAR(50)", true);
        assertTrue(comparator.equals("Hello", "  HELLO  ", col));
    }

    @Test
    void testCompareStringDifferent() {
        ColumnDef col = new ColumnDef("name", "VARCHAR(50)", true);
        assertFalse(comparator.equals("Alice", "Bob", col));
    }

    @Test
    void testCompareStringNulls() {
        ColumnDef col = new ColumnDef("name", "VARCHAR(50)", true);
        assertTrue(comparator.equals(null, null, col));
        assertFalse(comparator.equals("Alice", null, col));
    }

    @Test
    void testCompareRowsAllEqual() {
        List<ColumnDef> columns = List.of(
            new ColumnDef("id", "INTEGER", false),
            new ColumnDef("name", "VARCHAR", true)
        );
        Map<String, Object> left = Map.of("id", 1, "name", "Alice");
        Map<String, Object> right = Map.of("id", 1, "name", "Alice");
        assertTrue(comparator.compareRows(left, right, columns).isEmpty());
    }

    @Test
    void testCompareRowsWithDiffs() {
        List<ColumnDef> columns = List.of(
            new ColumnDef("id", "INTEGER", false),
            new ColumnDef("name", "VARCHAR", true),
            new ColumnDef("score", "INTEGER", true)
        );
        Map<String, Object> left = Map.of("id", 1, "name", "Alice", "score", 100);
        Map<String, Object> right = Map.of("id", 1, "name", "Bob", "score", 200);

        List<String> diffs = comparator.compareRows(left, right, columns);
        assertEquals(2, diffs.size());
        assertTrue(diffs.contains("name"));
        assertTrue(diffs.contains("score"));
    }

    @Test
    void testCustomComparator() {
        ColumnDef col = new ColumnDef("status", "VARCHAR", true);
        ColumnComparator alwaysMatch = new ColumnComparator(CompareOptions.defaults()) {
            @Override
            public boolean equals(Object left, Object right, String columnName) {
                return true;
            }
        };
        comparator.registerComparator("status", alwaysMatch);
        assertTrue(comparator.equals("active", "inactive", col));
    }

    @Test
    void testDefaultFallback() {
        ColumnDef col = new ColumnDef("json_data", "JSON", true);
        assertTrue(comparator.equals("{\"a\":1}", "{\"a\":1}", col));
        assertFalse(comparator.equals("{\"a\":1}", "{\"a\":2}", col));
    }
}
