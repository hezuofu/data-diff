package io.sketch.datadiff.util;

import io.sketch.datadiff.engine.SegmentSplitter;

/**
 * Utility for calculating ID ranges and sampling.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
public class IdRangeCalculator {
    
    private final SegmentSplitter splitter;
    
    public IdRangeCalculator() {
        this.splitter = new SegmentSplitter();
    }
    
    /**
     * Calculate min/max for a primary key column.
     */
    public static String queryMinMax(String tableName, String columnName) {
        return "SELECT MIN(%s) as min_val, MAX(%s) as max_val FROM %s".formatted(
            columnName, columnName, tableName
        );
    }
    
    /**
     * Calculate row count.
     */
    public static String queryCount(String tableName, String whereClause) {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        if (whereClause != null && !whereClause.isBlank()) {
            sql += " WHERE " + whereClause;
        }
        return sql;
    }
}
