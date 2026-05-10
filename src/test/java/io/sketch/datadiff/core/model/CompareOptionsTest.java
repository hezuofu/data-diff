package io.sketch.datadiff.core.model;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class CompareOptionsTest {

    @Test
    void testDefaultValues() {
        CompareOptions opts = CompareOptions.defaults();
        assertEquals(CompareOptions.StrategyType.AUTO, opts.getStrategy());
        assertEquals(32, opts.getBisectionFactor());
        assertEquals(16384, opts.getBisectionThreshold());
        assertEquals(4, opts.getThreads());
        assertEquals(0.0, opts.getNumericTolerance());
        assertFalse(opts.isDebug());
        assertFalse(opts.isIgnoreCase());
        assertTrue(opts.getExcludeColumns().isEmpty());
        assertTrue(opts.getCaseInsensitiveColumns().isEmpty());
        assertTrue(opts.isCaseSensitiveTable());
    }

    @Test
    void testFullBuilder() {
        CompareOptions opts = CompareOptions.builder()
            .leftUrl("jdbc:mysql://host1/db")
            .rightUrl("jdbc:mysql://host2/db")
            .leftTable("users_a")
            .rightTable("users_b")
            .keyColumn("id")
            .updateColumn("updated_at")
            .columns(List.of("id", "name", "age"))
            .whereClause("status = 'active'")
            .minKey(1)
            .maxKey(10000)
            .minUpdate(LocalDateTime.of(2025, 1, 1, 0, 0))
            .maxUpdate(LocalDateTime.of(2025, 12, 31, 23, 59))
            .bisectionFactor(64)
            .bisectionThreshold(8192)
            .threads(8)
            .debug(true)
            .numericTolerance(0.001)
            .ignoreCase(true)
            .excludeColumns("created_at", "updated_at")
            .caseInsensitiveColumns(Set.of("name"))
            .caseSensitiveTable(false)
            .strategy(CompareOptions.StrategyType.HASH)
            .build();

        assertEquals("jdbc:mysql://host1/db", opts.getLeftUrl());
        assertEquals("jdbc:mysql://host2/db", opts.getRightUrl());
        assertEquals("users_a", opts.getLeftTable());
        assertEquals("users_b", opts.getRightTable());
        assertEquals("id", opts.getKeyColumn());
        assertEquals("updated_at", opts.getUpdateColumn());
        assertEquals(3, opts.getColumns().size());
        assertEquals("status = 'active'", opts.getWhereClause());
        assertEquals(1, opts.getMinKey());
        assertEquals(10000, opts.getMaxKey());
        assertEquals(64, opts.getBisectionFactor());
        assertEquals(8192, opts.getBisectionThreshold());
        assertEquals(8, opts.getThreads());
        assertTrue(opts.isDebug());
        assertEquals(0.001, opts.getNumericTolerance());
        assertTrue(opts.isIgnoreCase());
        assertEquals(2, opts.getExcludeColumns().size());
        assertEquals(1, opts.getCaseInsensitiveColumns().size());
        assertFalse(opts.isCaseSensitiveTable());
        assertEquals(CompareOptions.StrategyType.HASH, opts.getStrategy());
    }

    @Test
    void testShouldExcludeColumn() {
        CompareOptions opts = CompareOptions.builder()
            .excludeColumns("meta", "internal")
            .build();
        assertTrue(opts.shouldExcludeColumn("meta"));
        assertTrue(opts.shouldExcludeColumn("internal"));
        assertFalse(opts.shouldExcludeColumn("name"));
    }

    @Test
    void testIsCaseInsensitive() {
        CompareOptions opts = CompareOptions.builder()
            .ignoreCase(true)
            .caseInsensitiveColumns(Set.of("comment"))
            .build();
        assertTrue(opts.isCaseInsensitive("name"));
        assertTrue(opts.isCaseInsensitive("comment"));
    }

    @Test
    void testIsCaseInsensitiveColumnOnly() {
        CompareOptions opts = CompareOptions.builder()
            .caseInsensitiveColumns(Set.of("comment"))
            .build();
        assertTrue(opts.isCaseInsensitive("comment"));
        assertFalse(opts.isCaseInsensitive("name"));
    }

    @Test
    void testInvalidBisectionFactor() {
        assertThrows(IllegalArgumentException.class, () ->
            CompareOptions.builder().bisectionFactor(0));
        assertThrows(IllegalArgumentException.class, () ->
            CompareOptions.builder().bisectionFactor(-1));
    }

    @Test
    void testInvalidBisectionThreshold() {
        assertThrows(IllegalArgumentException.class, () ->
            CompareOptions.builder().bisectionThreshold(0));
    }

    @Test
    void testInvalidThreads() {
        assertThrows(IllegalArgumentException.class, () ->
            CompareOptions.builder().threads(0));
    }

    @Test
    void testInvalidNumericTolerance() {
        assertThrows(IllegalArgumentException.class, () ->
            CompareOptions.builder().numericTolerance(-0.5));
    }

    @Test
    void testNullStrategyDefaultsToAuto() {
        CompareOptions opts = CompareOptions.builder().strategy(null).build();
        assertEquals(CompareOptions.StrategyType.AUTO, opts.getStrategy());
    }
}
