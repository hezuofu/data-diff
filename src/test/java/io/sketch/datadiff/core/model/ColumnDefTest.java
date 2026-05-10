package io.sketch.datadiff.core.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ColumnDefTest {

    @Test
    void testTypeChecks() {
        ColumnDef numeric = new ColumnDef("age", "INTEGER", false);
        assertTrue(numeric.isNumeric());
        assertFalse(numeric.isString());
        
        ColumnDef string = new ColumnDef("name", "VARCHAR(50)", true);
        assertTrue(string.isString());
        assertFalse(string.isNumeric());
        
        ColumnDef date = new ColumnDef("created_at", "TIMESTAMP", false);
        assertTrue(date.isDateTime());
    }

    @Test
    void testValidation() {
        assertThrows(IllegalArgumentException.class, () -> new ColumnDef("", "INT", false));
        assertThrows(IllegalArgumentException.class, () -> new ColumnDef("id", "", false));
    }
}
