package io.sketch.datadiff.comparator;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class StringNormalizingComparatorTest {

    @Test
    void testDefaultNormalization() {
        StringNormalizingComparator comparator = new StringNormalizingComparator();
        assertTrue(comparator.equals("  Hello  ", "HELLO"));
        assertTrue(comparator.equals("world", " WORLD "));
    }

    @Test
    void testCaseSensitiveOnly() {
        StringNormalizingComparator comparator = new StringNormalizingComparator(false, true);
        assertFalse(comparator.equals("Hello", "hello"));
        assertTrue(comparator.equals("  Hello  ", "Hello"));
    }

    @Test
    void testWhitespaceSensitiveOnly() {
        StringNormalizingComparator comparator = new StringNormalizingComparator(true, false);
        assertFalse(comparator.equals("  Hello  ", "Hello"));
        assertTrue(comparator.equals("Hello", "HELLO"));
    }

    @Test
    void testWithNulls() {
        StringNormalizingComparator comparator = new StringNormalizingComparator();
        assertTrue(comparator.equals(null, null));
        assertFalse(comparator.equals("hello", null));
        assertFalse(comparator.equals(null, "hello"));
    }
}
