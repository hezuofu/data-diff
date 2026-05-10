package io.sketch.datadiff.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MetricsCollectorTest {

    @Test
    void testStartStop() {
        MetricsCollector metrics = new MetricsCollector();
        metrics.start();
        metrics.stop();
        assertFalse(metrics.getDuration().isNegative());
    }

    @Test
    void testDurationWithoutStop() {
        MetricsCollector metrics = new MetricsCollector();
        metrics.start();
        assertFalse(metrics.getDuration().isNegative());
    }

    @Test
    void testDurationWithoutStart() {
        MetricsCollector metrics = new MetricsCollector();
        assertEquals(0, metrics.getDuration().toMillis());
    }

    @Test
    void testIncrementRows() {
        MetricsCollector metrics = new MetricsCollector();
        metrics.incrementRowsProcessed(100);
        metrics.incrementRowsProcessed(50);
        assertEquals(150, metrics.getRowsProcessed());
    }

    @Test
    void testIncrementSegments() {
        MetricsCollector metrics = new MetricsCollector();
        metrics.incrementSegmentsCompared();
        metrics.incrementSegmentsCompared();
        assertEquals(2, metrics.getSegmentsCompared());
    }

    @Test
    void testIncrementChecksums() {
        MetricsCollector metrics = new MetricsCollector();
        assertEquals(0, metrics.getChecksumsComputed());
        metrics.incrementChecksumsComputed();
        assertEquals(1, metrics.getChecksumsComputed());
    }

    @Test
    void testRowsPerSecond() {
        MetricsCollector metrics = new MetricsCollector();
        metrics.start();
        metrics.incrementRowsProcessed(1000);
        // Without waiting, the rate should be positive
        assertTrue(metrics.getRowsPerSecond() >= 0);
    }

    @Test
    void testRowsPerSecondZeroDuration() {
        MetricsCollector metrics = new MetricsCollector();
        assertEquals(0.0, metrics.getRowsPerSecond());
    }

    @Test
    void testToString() {
        MetricsCollector metrics = new MetricsCollector();
        String str = metrics.toString();
        assertTrue(str.contains("rows="));
        assertTrue(str.contains("segments="));
        assertTrue(str.contains("checksums="));
    }
}
