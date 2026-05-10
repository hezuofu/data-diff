package io.sketch.datadiff.core.model;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DiffRecordTest {

    @Test
    void testFactoryLeftOnly() {
        DiffRecord record = DiffRecord.leftOnly(
            Map.of("id", 1),
            Map.of("id", 1, "name", "Alice")
        );
        assertEquals(DiffType.LEFT_ONLY, record.diffType());
        assertEquals(Map.of("id", 1), record.primaryKey());
        assertEquals(Map.of("id", 1, "name", "Alice"), record.leftData());
        assertNull(record.rightData());
        assertTrue(record.differingColumns().isEmpty());
    }

    @Test
    void testFactoryRightOnly() {
        DiffRecord record = DiffRecord.rightOnly(
            Map.of("id", 2),
            Map.of("id", 2, "name", "Bob")
        );
        assertEquals(DiffType.RIGHT_ONLY, record.diffType());
        assertNull(record.leftData());
        assertNotNull(record.rightData());
    }

    @Test
    void testFactoryModified() {
        DiffRecord record = DiffRecord.modified(
            Map.of("id", 3),
            Map.of("id", 3, "name", "Alice"),
            Map.of("id", 3, "name", "Bob"),
            List.of("name")
        );
        assertEquals(DiffType.MODIFIED, record.diffType());
        assertEquals(List.of("name"), record.differingColumns());
    }

    @Test
    void testNullPrimaryKeyRejected() {
        assertThrows(IllegalArgumentException.class, () ->
            new DiffRecord(null, Map.of(), Map.of(), DiffType.MODIFIED, List.of())
        );
        assertThrows(IllegalArgumentException.class, () ->
            new DiffRecord(Map.of(), Map.of(), Map.of(), DiffType.MODIFIED, List.of())
        );
    }

    @Test
    void testNullDiffTypeRejected() {
        assertThrows(IllegalArgumentException.class, () ->
            new DiffRecord(Map.of("id", 1), Map.of(), Map.of(), null, List.of())
        );
    }

    @Test
    void testNullDifferingColumnsDefaultsToEmpty() {
        DiffRecord record = new DiffRecord(
            Map.of("id", 1), Map.of(), Map.of(), DiffType.MODIFIED, null
        );
        assertTrue(record.differingColumns().isEmpty());
    }

    @Test
    void testGetPrimaryKeyStringSingle() {
        DiffRecord record = DiffRecord.modified(
            Map.of("id", 42),
            Map.of("id", 42, "val", "a"),
            Map.of("id", 42, "val", "b"),
            List.of("val")
        );
        assertEquals("42", record.getPrimaryKeyString());
    }

    @Test
    void testGetPrimaryKeyStringComposite() {
        DiffRecord record = DiffRecord.modified(
            Map.of("id1", 1, "id2", 2),
            Map.of("id1", 1, "id2", 2, "val", "a"),
            Map.of("id1", 1, "id2", 2, "val", "b"),
            List.of("val")
        );
        String pkStr = record.getPrimaryKeyString();
        assertTrue(pkStr.contains("{"));
    }

    @Test
    void testToString() {
        DiffRecord record = DiffRecord.leftOnly(Map.of("id", 1), Map.of());
        String str = record.toString();
        assertTrue(str.contains("LEFT_ONLY"));
        assertTrue(str.contains("1"));
    }
}
