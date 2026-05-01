package io.sketch.datadiff.core.model;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

/**
 * Complete parameter contract for data diff comparison.
 * Aligned with Python data-diff parameter specifications.
 *
 * @author lanxia39@163.com
 */
public class CompareOptions {

    // ===== 策略选择 =====

    public enum StrategyType {
        HASH,
        JOIN,
        AUTO
    }

    // ===== 连接配置 =====

    private final String leftUrl;
    private final String rightUrl;
    private final String leftTable;
    private final String rightTable;

    // ===== 数据范围 =====

    private final String keyColumn;
    private final String updateColumn;
    private final List<String> columns;
    private final String whereClause;
    private final Comparable<?> minKey;
    private final Comparable<?> maxKey;
    private final LocalDateTime minUpdate;
    private final LocalDateTime maxUpdate;

    // ===== 算法调优 =====

    private final int bisectionFactor;
    private final int bisectionThreshold;
    private final int threads;
    private final boolean debug;

    // ===== 比对选项 =====

    private final double numericTolerance;
    private final boolean ignoreCase;
    private final List<String> excludeColumns;
    private final Set<String> caseInsensitiveColumns;
    private final boolean caseSensitiveTable;

    // ===== 策略选择 =====

    private final StrategyType strategy;

    private CompareOptions(Builder builder) {
        this.leftUrl = builder.leftUrl;
        this.rightUrl = builder.rightUrl;
        this.leftTable = builder.leftTable;
        this.rightTable = builder.rightTable;
        this.keyColumn = builder.keyColumn;
        this.updateColumn = builder.updateColumn;
        this.columns = builder.columns != null ? List.copyOf(builder.columns) : null;
        this.whereClause = builder.whereClause;
        this.minKey = builder.minKey;
        this.maxKey = builder.maxKey;
        this.minUpdate = builder.minUpdate;
        this.maxUpdate = builder.maxUpdate;
        this.bisectionFactor = builder.bisectionFactor;
        this.bisectionThreshold = builder.bisectionThreshold;
        this.threads = builder.threads;
        this.debug = builder.debug;
        this.numericTolerance = builder.numericTolerance;
        this.ignoreCase = builder.ignoreCase;
        this.excludeColumns = List.copyOf(builder.excludeColumns);
        this.caseInsensitiveColumns = Set.copyOf(builder.caseInsensitiveColumns);
        this.caseSensitiveTable = builder.caseSensitiveTable;
        this.strategy = builder.strategy;
    }

    // ===== Getters =====

    public String getLeftUrl() { return leftUrl; }
    public String getRightUrl() { return rightUrl; }
    public String getLeftTable() { return leftTable; }
    public String getRightTable() { return rightTable; }

    public String getKeyColumn() { return keyColumn; }
    public String getUpdateColumn() { return updateColumn; }
    public List<String> getColumns() { return columns; }
    public String getWhereClause() { return whereClause; }
    public Comparable<?> getMinKey() { return minKey; }
    public Comparable<?> getMaxKey() { return maxKey; }
    public LocalDateTime getMinUpdate() { return minUpdate; }
    public LocalDateTime getMaxUpdate() { return maxUpdate; }

    public int getBisectionFactor() { return bisectionFactor; }
    public int getBisectionThreshold() { return bisectionThreshold; }
    public int getThreads() { return threads; }
    public boolean isDebug() { return debug; }

    public double getNumericTolerance() { return numericTolerance; }
    public boolean isIgnoreCase() { return ignoreCase; }
    public List<String> getExcludeColumns() { return excludeColumns; }
    public Set<String> getCaseInsensitiveColumns() { return caseInsensitiveColumns; }
    public boolean isCaseSensitiveTable() { return caseSensitiveTable; }

    public StrategyType getStrategy() { return strategy; }

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
        return ignoreCase || caseInsensitiveColumns.contains(columnName);
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
        private String leftUrl;
        private String rightUrl;
        private String leftTable;
        private String rightTable;
        private String keyColumn;
        private String updateColumn;
        private List<String> columns;
        private String whereClause;
        private Comparable<?> minKey;
        private Comparable<?> maxKey;
        private LocalDateTime minUpdate;
        private LocalDateTime maxUpdate;
        private int bisectionFactor = 32;
        private int bisectionThreshold = 16384;
        private int threads = 4;
        private boolean debug;
        private double numericTolerance;
        private boolean ignoreCase;
        private List<String> excludeColumns = List.of();
        private Set<String> caseInsensitiveColumns = Set.of();
        private boolean caseSensitiveTable = true;
        private StrategyType strategy = StrategyType.AUTO;

        private Builder() {}

        // ===== 连接配置 =====

        public Builder leftUrl(String leftUrl) { this.leftUrl = leftUrl; return this; }
        public Builder rightUrl(String rightUrl) { this.rightUrl = rightUrl; return this; }
        public Builder leftTable(String leftTable) { this.leftTable = leftTable; return this; }
        public Builder rightTable(String rightTable) { this.rightTable = rightTable; return this; }

        // ===== 数据范围 =====

        public Builder keyColumn(String keyColumn) { this.keyColumn = keyColumn; return this; }
        public Builder updateColumn(String updateColumn) { this.updateColumn = updateColumn; return this; }
        public Builder columns(List<String> columns) { this.columns = columns; return this; }
        public Builder whereClause(String whereClause) { this.whereClause = whereClause; return this; }
        public Builder minKey(Comparable<?> minKey) { this.minKey = minKey; return this; }
        public Builder maxKey(Comparable<?> maxKey) { this.maxKey = maxKey; return this; }
        public Builder minUpdate(LocalDateTime minUpdate) { this.minUpdate = minUpdate; return this; }
        public Builder maxUpdate(LocalDateTime maxUpdate) { this.maxUpdate = maxUpdate; return this; }

        // ===== 算法调优 =====

        public Builder bisectionFactor(int bisectionFactor) {
            if (bisectionFactor < 1) throw new IllegalArgumentException("bisectionFactor must be >= 1");
            this.bisectionFactor = bisectionFactor;
            return this;
        }

        public Builder bisectionThreshold(int bisectionThreshold) {
            if (bisectionThreshold < 1) throw new IllegalArgumentException("bisectionThreshold must be >= 1");
            this.bisectionThreshold = bisectionThreshold;
            return this;
        }

        public Builder threads(int threads) {
            if (threads < 1) throw new IllegalArgumentException("threads must be >= 1");
            this.threads = threads;
            return this;
        }

        public Builder debug(boolean debug) { this.debug = debug; return this; }

        // ===== 比对选项 =====

        public Builder numericTolerance(double numericTolerance) {
            if (numericTolerance < 0.0) throw new IllegalArgumentException("numericTolerance must be >= 0");
            this.numericTolerance = numericTolerance;
            return this;
        }

        public Builder ignoreCase(boolean ignoreCase) { this.ignoreCase = ignoreCase; return this; }

        public Builder excludeColumns(List<String> excludeColumns) {
            this.excludeColumns = excludeColumns != null ? List.copyOf(excludeColumns) : List.of();
            return this;
        }

        public Builder excludeColumns(String... excludeColumns) {
            this.excludeColumns = List.of(excludeColumns);
            return this;
        }

        public Builder caseInsensitiveColumns(Set<String> caseInsensitiveColumns) {
            this.caseInsensitiveColumns = caseInsensitiveColumns != null ? Set.copyOf(caseInsensitiveColumns) : Set.of();
            return this;
        }

        public Builder caseSensitiveTable(boolean caseSensitiveTable) {
            this.caseSensitiveTable = caseSensitiveTable;
            return this;
        }

        // ===== 策略选择 =====

        public Builder strategy(StrategyType strategy) {
            this.strategy = strategy != null ? strategy : StrategyType.AUTO;
            return this;
        }

        public CompareOptions build() {
            return new CompareOptions(this);
        }
    }
}
