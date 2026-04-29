package io.sketch.datadiff.comparator;

import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.core.model.ColumnDef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Composite comparator that applies different comparators based on column types.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class CompositeComparator {
    
    private final Map<String, ColumnComparator> comparators;
    private final ColumnComparator defaultComparator;
    
    public CompositeComparator(CompareOptions options) {
        this.comparators = new HashMap<>();
        this.defaultComparator = new ColumnComparator(options);
    }
    
    /**
     * Register a custom comparator for a specific column.
     */
    public void registerComparator(String columnName, ColumnComparator comparator) {
        comparators.put(columnName, comparator);
    }
    
    /**
     * Compare two column values using appropriate comparator.
     */
    public boolean equals(Object left, Object right, ColumnDef columnDef) {
        String columnName = columnDef.name();
        
        // Use custom comparator if registered
        ColumnComparator customComparator = comparators.get(columnName);
        if (customComparator != null) {
            return customComparator.equals(left, right, columnName);
        }
        
        // Use type-specific comparator
        if (columnDef.isNumeric()) {
            return compareNumeric(left, right);
        } else if (columnDef.isString()) {
            return compareString(left, right);
        }
        
        // Use default comparator
        return defaultComparator.equals(left, right, columnName);
    }
    
    /**
     * Compare two rows using composite comparators.
     */
    public List<String> compareRows(Map<String, Object> leftRow, Map<String, Object> rightRow, List<ColumnDef> columns) {
        List<String> differingColumns = new ArrayList<>();
        
        for (ColumnDef columnDef : columns) {
            String columnName = columnDef.name();
            Object leftValue = leftRow.get(columnName);
            Object rightValue = rightRow.get(columnName);
            
            if (!equals(leftValue, rightValue, columnDef)) {
                differingColumns.add(columnName);
            }
        }
        
        return differingColumns;
    }
    
    private boolean compareNumeric(Object left, Object right) {
        if (left == null && right == null) return true;
        if (left == null || right == null) return false;
        
        double leftNum = ((Number) left).doubleValue();
        double rightNum = ((Number) right).doubleValue();
        
        return Math.abs(leftNum - rightNum) < 0.0001; // Default tolerance
    }
    
    private boolean compareString(Object left, Object right) {
        if (left == null && right == null) return true;
        if (left == null || right == null) return false;
        
        return left.toString().trim().equalsIgnoreCase(right.toString().trim());
    }
}
