package io.sketch.datadiff.core.model;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class TableInfoTest {

    @Test
    void testGetFullName() {
        TableInfo table = new TableInfo("users", List.of(new ColumnDef("id", "INT", false)), "id");
        assertEquals("users", table.getFullName());
        
        TableInfo tableWithSchema = new TableInfo("users", "public", 
            List.of(new ColumnDef("id", "INT", false)), List.of("id"), List.of());
        assertEquals("public.users", tableWithSchema.getFullName());
    }

    @Test
    void testGetColumn() {
        ColumnDef idCol = new ColumnDef("id", "INT", false);
        TableInfo table = new TableInfo("users", List.of(idCol), "id");
        
        assertEquals(idCol, table.getColumn("id"));
        assertThrows(IllegalArgumentException.class, () -> table.getColumn("missing"));
    }

    @Test
    void testCompositeKey() {
        TableInfo table = new TableInfo("users", 
            List.of(new ColumnDef("id1", "INT", false), new ColumnDef("id2", "INT", false)), 
            List.of("id1", "id2"));
        assertTrue(table.isCompositeKey());
    }
}
