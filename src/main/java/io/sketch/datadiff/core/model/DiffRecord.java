package io.sketch.datadiff.core.model;

import java.util.Map;

/**
 * Record representing a single difference found during comparison.
 * 
 * @param primaryKey primary key value(s) as a map
 * @param leftData row data from left table (null if LEFT_ONLY)
 * @param rightData row data from right table (null if RIGHT_ONLY)
 * @param diffType type of difference
 * @param differingColumns list of column names that differ (for MODIFIED type)
 *
 * @author lanxia39@163.com
 */
public record DiffRecord(
    Map<String, Object> primaryKey,
    Map<String, Object> leftData,
    Map<String, Object> rightData,
    DiffType diffType,
    java.util.List<String> differingColumns
) {
    public DiffRecord {
        if (primaryKey == null || primaryKey.isEmpty()) {
            throw new IllegalArgumentException("Primary key cannot be null or empty");
        }
        if (diffType == null) {
            throw new IllegalArgumentException("Diff type cannot be null");
        }
        differingColumns = (differingColumns == null) ? java.util.List.of() : java.util.List.copyOf(differingColumns);
    }
    
    /**
     * Convenience constructor for LEFT_ONLY diff.
     */
    public static DiffRecord leftOnly(Map<String, Object> primaryKey, Map<String, Object> leftData) {
        return new DiffRecord(primaryKey, leftData, null, DiffType.LEFT_ONLY, java.util.List.of());
    }
    
    /**
     * Convenience constructor for RIGHT_ONLY diff.
     */
    public static DiffRecord rightOnly(Map<String, Object> primaryKey, Map<String, Object> rightData) {
        return new DiffRecord(primaryKey, null, rightData, DiffType.RIGHT_ONLY, java.util.List.of());
    }
    
    /**
     * Convenience constructor for MODIFIED diff.
     */
    public static DiffRecord modified(
        Map<String, Object> primaryKey, 
        Map<String, Object> leftData, 
        Map<String, Object> rightData,
        java.util.List<String> differingColumns
    ) {
        return new DiffRecord(primaryKey, leftData, rightData, DiffType.MODIFIED, differingColumns);
    }
    
    /**
     * Get primary key as a string representation.
     */
    public String getPrimaryKeyString() {
        if (primaryKey.size() == 1) {
            return primaryKey.values().iterator().next().toString();
        }
        return primaryKey.toString();
    }
    
    @Override
    public String toString() {
        return "DiffRecord[%s, type=%s]".formatted(getPrimaryKeyString(), diffType);
    }
}
