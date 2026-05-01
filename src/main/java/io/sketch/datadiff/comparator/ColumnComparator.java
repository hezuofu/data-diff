package io.sketch.datadiff.comparator;

import io.sketch.datadiff.core.model.CompareOptions;

/**
 * Column-level comparator with support for tolerance and normalization.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class ColumnComparator {
    
    private final CompareOptions options;
    
    public ColumnComparator(CompareOptions options) {
        this.options = options;
    }
    
    /**
     * Compare two column values.
     * 
     * @return true if values are equal (considering tolerance/normalization)
     */
    public boolean equals(Object left, Object right, String columnName) {
        // Check if column should be excluded
        if (options.shouldExcludeColumn(columnName)) {
            return true;
        }
        
        // Handle nulls
        if (left == null && right == null) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        
        // Check if case-insensitive comparison (global or column-level)
        if (options.isCaseInsensitive(columnName)) {
            return left.toString().equalsIgnoreCase(right.toString());
        }
        
        // Check if numeric tolerance applies
        if (options.getNumericTolerance() > 0.0 && isNumeric(left) && isNumeric(right)) {
            double leftNum = ((Number) left).doubleValue();
            double rightNum = ((Number) right).doubleValue();
            return Math.abs(leftNum - rightNum) <= options.getNumericTolerance();
        }
        
        // Default equality check
        return java.util.Objects.equals(left, right);
    }
    
    private boolean isNumeric(Object value) {
        return value instanceof Number;
    }
}
