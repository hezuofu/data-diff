package io.sketch.datadiff.core.model;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DiffResultTest {

    @Test
    void testHasDifferences() {
        DiffResult empty = new DiffResult(
            List.of(),
            DiffResult.Statistics.empty(),
            Duration.ZERO
        );
        assertFalse(empty.hasDifferences());

        DiffRecord record = DiffRecord.modified(
            Map.of("id", 1),
            Map.of("id", 1, "val", "a"),
            Map.of("id", 1, "val", "b"),
            List.of("val")
        );
        DiffResult withDiffs = new DiffResult(
            List.of(record),
            DiffResult.Statistics.empty(),
            Duration.ZERO
        );
        assertTrue(withDiffs.hasDifferences());
    }

    @Test
    void testGetDiffCount() {
        DiffResult result = new DiffResult(
            List.of(
                DiffRecord.leftOnly(Map.of("id", 1), Map.of()),
                DiffRecord.rightOnly(Map.of("id", 2), Map.of()),
                DiffRecord.modified(Map.of("id", 3), Map.of(), Map.of(), List.of())
            ),
            DiffResult.Statistics.empty(),
            Duration.ofSeconds(5)
        );
        assertEquals(3, result.getDiffCount());
    }

    @Test
    void testGetDiffCounts() {
        DiffResult result = new DiffResult(
            List.of(
                DiffRecord.leftOnly(Map.of("id", 1), Map.of()),
                DiffRecord.leftOnly(Map.of("id", 2), Map.of()),
                DiffRecord.rightOnly(Map.of("id", 3), Map.of()),
                DiffRecord.modified(Map.of("id", 4), Map.of(), Map.of(), List.of())
            ),
            DiffResult.Statistics.empty(),
            Duration.ofSeconds(5)
        );

        var counts = result.getDiffCounts();
        assertEquals(2L, counts.get(DiffType.LEFT_ONLY));
        assertEquals(1L, counts.get(DiffType.RIGHT_ONLY));
        assertEquals(1L, counts.get(DiffType.MODIFIED));
    }

    @Test
    void testGetDuration() {
        Duration d = Duration.ofMillis(1234);
        assertEquals(d, new DiffResult(List.of(), DiffResult.Statistics.empty(), d).getDuration());
    }

    @Test
    void testToString() {
        DiffResult result = new DiffResult(List.of(), DiffResult.Statistics.empty(), Duration.ofSeconds(3));
        assertTrue(result.toString().contains("differences=0"));
    }

    @Test
    void testStatisticsValidation() {
        assertThrows(IllegalArgumentException.class, () ->
            new DiffResult.Statistics(-1, 0, 0, 0, 0, 0)
        );
        assertThrows(IllegalArgumentException.class, () ->
            new DiffResult.Statistics(0, -1, 0, 0, 0, 0)
        );
        assertThrows(IllegalArgumentException.class, () ->
            new DiffResult.Statistics(0, 0, -1, 0, 0, 0)
        );
        assertThrows(IllegalArgumentException.class, () ->
            new DiffResult.Statistics(0, 0, 0, -1, 0, 0)
        );
    }

    @Test
    void testStatisticsEmpty() {
        var stats = DiffResult.Statistics.empty();
        assertEquals(0, stats.leftRowCount());
        assertEquals(0, stats.rightRowCount());
        assertEquals(0, stats.segmentsCompared());
        assertEquals(0, stats.segmentsMismatched());
        assertEquals(0, stats.maxBisectionDepth());
        assertEquals(0, stats.bisectionIterations());
    }

    @Test
    void testDiffRecordsImmutability() {
        DiffRecord record = DiffRecord.leftOnly(Map.of("id", 1), Map.of());
        List<DiffRecord> mutableList = new java.util.ArrayList<>(List.of(record));
        DiffResult result = new DiffResult(mutableList, DiffResult.Statistics.empty(), Duration.ZERO);
        mutableList.clear();
        assertEquals(1, result.getDiffCount());
        assertThrows(UnsupportedOperationException.class, () -> result.getDiffRecords().clear());
    }
}
