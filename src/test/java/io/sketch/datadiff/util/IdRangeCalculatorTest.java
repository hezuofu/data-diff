package io.sketch.datadiff.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class IdRangeCalculatorTest {

    @Test
    void testQueryMinMax() {
        String sql = IdRangeCalculator.queryMinMax("users", "id");
        assertTrue(sql.contains("MIN(id)"));
        assertTrue(sql.contains("MAX(id)"));
        assertTrue(sql.contains("FROM users"));
    }

    @Test
    void testQueryCount() {
        String sql = IdRangeCalculator.queryCount("users", "age > 18");
        assertEquals("SELECT COUNT(*) FROM users WHERE age > 18", sql);
        
        assertEquals("SELECT COUNT(*) FROM users", IdRangeCalculator.queryCount("users", null));
    }
}
