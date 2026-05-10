package io.sketch.datadiff.datasource.dialect;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class DialectResolverTest {

    @Test
    void testResolveBuiltIn() {
        assertTrue(DialectResolver.resolve("jdbc:mysql://localhost:3306/db") instanceof MySqlDialect);
        assertTrue(DialectResolver.resolve("jdbc:postgresql://localhost:5432/db") instanceof PostgreSqlDialect);
        assertTrue(DialectResolver.resolve("jdbc:h2:mem:test") instanceof H2Dialect);
    }

    @Test
    void testResolveUnknown() {
        assertThrows(RuntimeException.class, () -> DialectResolver.resolve("jdbc:unknown://host"));
    }

    @Test
    void testRegisterDialect() {
        SqlDialect mockDialect = new MySqlDialect();
        DialectResolver.registerDialect("jdbc:mock:", mockDialect);
        assertEquals(mockDialect, DialectResolver.resolve("jdbc:mock://host"));
    }
}
