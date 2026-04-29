package io.sketch.datadiff.core.model;

import java.util.List;
import java.util.Set;

/**
 * Configuration options for comparison operations.
 */
public class CompareOptions {
    
    /** Comparison algorithm to use */
    public enum Algorithm {
        HASHDIFF,  // Hash-based comparison with bisection
        JOINDIFF   // JOIN-based comparison (same database only)
    }
    
    private final Algorithm algorithm;
    private final int parallelism;
    private final long segmentSize;
    private final int maxBisectionDepth;
    private final List<String> excludeColumns;
    private final Set<String> caseInsensitiveColumns;
    private final double numericTolerance;
    private final boolean useChecksumCache;
    private final int cacheMaxSize;
    
    private CompareOptions(Builder builder) {
        this.algorithm = builder.algorithm;
        this.parallelism = builder.parallelism;
        this.segmentSize = builder.segmentSize;
        this.maxBisectionDepth = builder.maxBisectionDepth;
        this.excludeColumns = List.copyOf(builder.excludeColumns);
        this.caseInsensitiveColumns = Set.copyOf(builder.caseInsensitiveColumns);
        this.numericTolerance = builder.numericTolerance;
        this.useChecksumCache = builder.useChecksumCache;
        this.cacheMaxSize = builder.cacheMaxSize;
    }
    
    public Algorithm getAlgorithm() {
        return algorithm;
    }
    
    public int getParallelism() {
        return parallelism;
    }
    
    public long getSegmentSize() {
        return segmentSize;
    }
    
    public int getMaxBisectionDepth() {
        return maxBisectionDepth;
    }
    
    public List<String> getExcludeColumns() {
        return excludeColumns;
    }
    
    public Set<String> getCaseInsensitiveColumns() {
        return caseInsensitiveColumns;
    }
    
    public double getNumericTolerance() {
        return numericTolerance;
    }
    
    public boolean useChecksumCache() {
        return useChecksumCache;
    }
    
    public int getCacheMaxSize() {
        return cacheMaxSize;
    }
    
    /**
     * Check if a column should be excluded from comparison.
     */
    public boolean shouldExcludeColumn(String columnName) {
        return excludeColumns.contains(columnName);
    }
    
    /**
     * Check if a column should be compared case-insensitively.
     */
    public boolean isCaseInsensitive(String columnName) {
        return caseInsensitiveColumns.contains(columnName);
    }
    
    /**
     * Create a new builder with default options.
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Get default options.
     */
    public static CompareOptions defaults() {
        return builder().build();
    }
    
    /**
     * Builder for CompareOptions.
     */
    public static class Builder {
        private Algorithm algorithm = Algorithm.HASHDIFF;
        private int parallelism = Runtime.getRuntime().availableProcessors();
        private long segmentSize = 50000;
        private int maxBisectionDepth = 32;
        private List<String> excludeColumns = List.of();
        private Set<String> caseInsensitiveColumns = Set.of();
        private double numericTolerance = 0.0;
        private boolean useChecksumCache = true;
        private int cacheMaxSize = 10000;
        
        private Builder() {}
        
        public Builder algorithm(Algorithm algorithm) {
            this.algorithm = algorithm;
            return this;
        }
        
        public Builder parallelism(int parallelism) {
            if (parallelism < 1) {
                throw new IllegalArgumentException("Parallelism must be >= 1");
            }
            this.parallelism = parallelism;
            return this;
        }
        
        public Builder segmentSize(long segmentSize) {
            if (segmentSize < 100) {
                throw new IllegalArgumentException("Segment size must be >= 100");
            }
            this.segmentSize = segmentSize;
            return this;
        }
        
        public Builder maxBisectionDepth(int maxBisectionDepth) {
            if (maxBisectionDepth < 1 || maxBisectionDepth > 64) {
                throw new IllegalArgumentException("Max bisection depth must be between 1 and 64");
            }
            this.maxBisectionDepth = maxBisectionDepth;
            return this;
        }
        
        public Builder excludeColumns(List<String> excludeColumns) {
            this.excludeColumns = List.copyOf(excludeColumns);
            return this;
        }
        
        public Builder excludeColumns(String... excludeColumns) {
            this.excludeColumns = List.of(excludeColumns);
            return this;
        }
        
        public Builder caseInsensitiveColumns(Set<String> caseInsensitiveColumns) {
            this.caseInsensitiveColumns = Set.copyOf(caseInsensitiveColumns);
            return this;
        }
        
        public Builder numericTolerance(double numericTolerance) {
            if (numericTolerance < 0.0) {
                throw new IllegalArgumentException("Numeric tolerance must be >= 0");
            }
            this.numericTolerance = numericTolerance;
            return this;
        }
        
        public Builder useChecksumCache(boolean useChecksumCache) {
            this.useChecksumCache = useChecksumCache;
            return this;
        }
        
        public Builder cacheMaxSize(int cacheMaxSize) {
            if (cacheMaxSize < 100) {
                throw new IllegalArgumentException("Cache max size must be >= 100");
            }
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }
        
        public CompareOptions build() {
            return new CompareOptions(this);
        }
    }
}
