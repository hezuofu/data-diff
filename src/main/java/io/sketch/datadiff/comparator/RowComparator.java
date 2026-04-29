package io.sketch.datadiff.comparator;

import io.sketch.datadiff.core.model.CompareOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Row-level comparator that compares entire rows.
 */
public class RowComparator {
    
    private final ColumnComparator columnComparator;
    
    public RowComparator(CompareOptions options) {
        this.columnComparator = new ColumnComparator(options);
    }
    
    /**
     * Compare two rows and return list of differing column names.
     * 
     * @return empty list if rows are equal, otherwise list of differing columns
     */
    public List<String> compare(Map<String, Object> leftRow, Map<String, Object> rightRow) {
        List<String> differingColumns = new ArrayList<>();
        
        // Compare all columns in left row
        for (Map.Entry<String, Object> entry : leftRow.entrySet()) {
            String columnName = entry.getKey();
            Object leftValue = entry.getValue();
            Object rightValue = rightRow.get(columnName);
            
            if (!columnComparator.equals(leftValue, rightValue, columnName)) {
                differingColumns.add(columnName);
            }
        }
        
        // Check for extra columns in right row
        for (String columnName : rightRow.keySet()) {
            if (!leftRow.containsKey(columnName)) {
                differingColumns.add(columnName);
            }
        }
        
        return differingColumns;
    }
    
    /**
     * Check if two rows are equal.
     */
    public boolean isEqual(Map<String, Object> leftRow, Map<String, Object> rightRow) {
        return compare(leftRow, rightRow).isEmpty();
    }
}
