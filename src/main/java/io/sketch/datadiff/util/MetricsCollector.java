package io.sketch.datadiff.util;

import java.time.Duration;
import java.time.Instant;

/**
 * Utility for collecting metrics during comparison.
 */
public class MetricsCollector {
    
    private Instant startTime;
    private Instant endTime;
    private long rowsProcessed = 0;
    private long segmentsCompared = 0;
    private long checksumsComputed = 0;
    
    public void start() {
        startTime = Instant.now();
    }
    
    public void stop() {
        endTime = Instant.now();
    }
    
    public void incrementRowsProcessed(long count) {
        rowsProcessed += count;
    }
    
    public void incrementSegmentsCompared() {
        segmentsCompared++;
    }
    
    public void incrementChecksumsComputed() {
        checksumsComputed++;
    }
    
    public Duration getDuration() {
        if (startTime == null) return Duration.ZERO;
        Instant end = endTime != null ? endTime : Instant.now();
        return Duration.between(startTime, end);
    }
    
    public double getRowsPerSecond() {
        Duration duration = getDuration();
        if (duration.isZero()) return 0;
        return rowsProcessed / (duration.toMillis() / 1000.0);
    }
    
    public long getRowsProcessed() {
        return rowsProcessed;
    }
    
    public long getSegmentsCompared() {
        return segmentsCompared;
    }
    
    public long getChecksumsComputed() {
        return checksumsComputed;
    }
    
    @Override
    public String toString() {
        return "Metrics[duration=%s, rows=%d, segments=%d, checksums=%d, rate=%.2f rows/s]".formatted(
            getDuration(), rowsProcessed, segmentsCompared, checksumsComputed, getRowsPerSecond()
        );
    }
}
