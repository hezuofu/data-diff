package io.sketch.datadiff.comparator;

import io.sketch.datadiff.core.model.CompareOptions;
import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class RowComparatorTest {

    @Test
    void testEqualRows() {
        CompareOptions options = CompareOptions.builder().build();
        RowComparator comparator = new RowComparator(options);
        
        Map<String, Object> row1 = Map.of("id", 1, "name", "Alice");
        Map<String, Object> row2 = Map.of("id", 1, "name", "Alice");
        
        assertTrue(comparator.isEqual(row1, row2));
        assertTrue(comparator.compare(row1, row2).isEmpty());
    }

    @Test
    void testDifferingRows() {
        CompareOptions options = CompareOptions.builder().build();
        RowComparator comparator = new RowComparator(options);
        
        Map<String, Object> row1 = Map.of("id", 1, "name", "Alice");
        Map<String, Object> row2 = Map.of("id", 1, "name", "Bob");
        
        assertFalse(comparator.isEqual(row1, row2));
        List<String> diffs = comparator.compare(row1, row2);
        assertEquals(1, diffs.size());
        assertEquals("name", diffs.get(0));
    }

    @Test
    void testComplexTypes() {
        CompareOptions options = CompareOptions.builder().build();
        RowComparator comparator = new RowComparator(options);
        
        Map<String, Object> row1 = Map.of("data", Map.of("key", "val"), "list", List.of(1, 2));
        Map<String, Object> row2 = Map.of("data", Map.of("key", "val"), "list", List.of(1, 2));
        
        assertTrue(comparator.isEqual(row1, row2));
    }

    @Test
    void testNumericToleranceDeep() {
        CompareOptions options = CompareOptions.builder()
            .numericTolerance(0.001)
            .build();
        RowComparator comparator = new RowComparator(options);
        
        Map<String, Object> row1 = Map.of("price", 100.0001);
        Map<String, Object> row2 = Map.of("price", 100.0002);
        
        assertTrue(comparator.isEqual(row1, row2));
    }

    @Test
    void testMissingColumnInOneRow() {
        CompareOptions options = CompareOptions.builder().build();
        RowComparator comparator = new RowComparator(options);
        
        Map<String, Object> row1 = Map.of("id", 1, "extra", "val");
        Map<String, Object> row2 = Map.of("id", 1);
        
        assertFalse(comparator.isEqual(row1, row2));
        List<String> diffs = comparator.compare(row1, row2);
        assertTrue(diffs.contains("extra"));
    }
}
