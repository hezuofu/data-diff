package io.sketch.datadiff.datasource.dialect;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class MySqlDialectTest {

    private final MySqlDialect dialect = new MySqlDialect();

    @Test
    void testQuoteIdentifier() {
        assertEquals("`users`", dialect.quoteIdentifier("users"));
    }

    @Test
    void testHashExpression() {
        String expr = dialect.hashExpression(List.of("id", "name"));
        assertTrue(expr.contains("MD5"));
        assertTrue(expr.contains("CONCAT"));
        assertTrue(expr.contains("IFNULL"));
    }

    @Test
    void testCountQuery() {
        String sql = dialect.countQuery("users", "id > 10");
        assertEquals("SELECT COUNT(*) FROM `users` WHERE id > 10", sql);
    }
}
