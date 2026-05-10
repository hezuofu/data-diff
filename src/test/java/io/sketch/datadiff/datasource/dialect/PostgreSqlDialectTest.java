package io.sketch.datadiff.datasource.dialect;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PostgreSqlDialectTest {

    private final PostgreSqlDialect dialect = new PostgreSqlDialect();

    @Test
    void testQuoteIdentifier() {
        assertEquals("\"users\"", dialect.quoteIdentifier("users"));
    }

    @Test
    void testHashExpression() {
        String expr = dialect.hashExpression(List.of("id", "name"));
        assertTrue(expr.contains("hashtext"));
        assertTrue(expr.contains("COALESCE"));
        assertTrue(expr.contains("::bigint"));
    }

    @Test
    void testGetDriverClassName() {
        assertEquals("org.postgresql.Driver", dialect.getDriverClassName());
    }

    @Test
    void testGetDialectName() {
        assertEquals("PostgreSQL", dialect.getDialectName());
    }
}
