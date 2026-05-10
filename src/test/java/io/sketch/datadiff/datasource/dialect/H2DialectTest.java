package io.sketch.datadiff.datasource.dialect;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class H2DialectTest {

    private final H2Dialect dialect = new H2Dialect();

    @Test
    void testQuoteIdentifier() {
        assertEquals("\"users\"", dialect.quoteIdentifier("users"));
    }

    @Test
    void testHashExpression() {
        String expr = dialect.hashExpression(List.of("id", "val"));
        assertTrue(expr.contains("HASH"));
        assertTrue(expr.contains("IFNULL"));
        assertTrue(expr.contains("AS BIGINT"));
    }

    @Test
    void testGetDriverClassName() {
        assertEquals("org.h2.Driver", dialect.getDriverClassName());
    }

    @Test
    void testGetDialectName() {
        assertEquals("H2", dialect.getDialectName());
    }

    @Test
    void testCountQuery() {
        String sql = dialect.countQuery("users", null);
        assertEquals("SELECT COUNT(*) FROM \"users\"", sql);

        String filtered = dialect.countQuery("users", "id > 0");
        assertEquals("SELECT COUNT(*) FROM \"users\" WHERE id > 0", filtered);
    }

    @Test
    void testChecksumQuery() {
        String sql = dialect.checksumQuery(
            "users",
            List.of("id", "name"),
            "id",
            1,
            100
        );
        assertTrue(sql.contains("SUM"));
        assertTrue(sql.contains("checksum"));
        assertTrue(sql.contains("row_count"));
        assertTrue(sql.contains("BETWEEN 1 AND 100"));
    }
}
