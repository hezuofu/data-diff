package io.sketch.datadiff.core.model;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Comparison result containing diff records and statistics.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class DiffResult {
    
    private final List<DiffRecord> diffRecords;
    private final Statistics statistics;
    private final Duration duration;
    
    public DiffResult(List<DiffRecord> diffRecords, Statistics statistics, Duration duration) {
        this.diffRecords = List.copyOf(diffRecords);
        this.statistics = statistics;
        this.duration = duration;
    }
    
    /**
     * Get all diff records.
     */
    public List<DiffRecord> getDiffRecords() {
        return diffRecords;
    }
    
    /**
     * Get comparison statistics.
     */
    public Statistics getStatistics() {
        return statistics;
    }
    
    /**
     * Get comparison duration.
     */
    public Duration getDuration() {
        return duration;
    }
    
    /**
     * Check if there are any differences.
     */
    public boolean hasDifferences() {
        return !diffRecords.isEmpty();
    }
    
    /**
     * Get count of differences by type.
     */
    public Map<DiffType, Long> getDiffCounts() {
        return diffRecords.stream()
            .collect(java.util.stream.Collectors.groupingBy(
                DiffRecord::diffType, 
                java.util.stream.Collectors.counting()
            ));
    }
    
    /**
     * Get total number of differences.
     */
    public long getDiffCount() {
        return diffRecords.size();
    }
    
    @Override
    public String toString() {
        return "DiffResult[differences=%d, duration=%s]".formatted(
            getDiffCount(), duration
        );
    }
    
    /**
     * Statistics about the comparison operation.
     */
    public record Statistics(
        long leftRowCount,
        long rightRowCount,
        long segmentsCompared,
        long segmentsMismatched,
        long maxBisectionDepth,
        int bisectionIterations
    ) {
        public Statistics {
            if (leftRowCount < 0 || rightRowCount < 0) {
                throw new IllegalArgumentException("Row counts cannot be negative");
            }
            if (segmentsCompared < 0 || segmentsMismatched < 0) {
                throw new IllegalArgumentException("Segment counts cannot be negative");
            }
            if (maxBisectionDepth < 0 || bisectionIterations < 0) {
                throw new IllegalArgumentException("Bisection metrics cannot be negative");
            }
        }
        
        /**
         * Create empty statistics.
         */
        public static Statistics empty() {
            return new Statistics(0, 0, 0, 0, 0, 0);
        }
    }
}
