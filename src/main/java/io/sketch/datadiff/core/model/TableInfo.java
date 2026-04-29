package io.sketch.datadiff.core.model;

import java.util.List;

/**
 * Table metadata record.
 * 
 * @param tableName table name
 * @param schemaName schema name (optional, may be null)
 * @param columns list of column definitions
 * @param primaryKey primary key column name(s)
 * @param indexes list of indexed column names
 *
 * @author lanxia39@163.com
 */
public record TableInfo(
    String tableName,
    String schemaName,
    List<ColumnDef> columns,
    List<String> primaryKey,
    List<String> indexes
) {
    public TableInfo {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns cannot be null or empty");
        }
        if (primaryKey == null || primaryKey.isEmpty()) {
            throw new IllegalArgumentException("Primary key cannot be null or empty");
        }
        indexes = (indexes == null) ? List.of() : List.copyOf(indexes);
    }
    
    /**
     * Convenience constructor without schema.
     */
    public TableInfo(String tableName, List<ColumnDef> columns, List<String> primaryKey) {
        this(tableName, null, columns, primaryKey, List.of());
    }
    
    /**
     * Convenience constructor without schema and indexes.
     */
    public TableInfo(String tableName, List<ColumnDef> columns, String primaryKey) {
        this(tableName, null, columns, List.of(primaryKey), List.of());
    }
    
    /**
     * Get full table name with schema prefix if available.
     */
    public String getFullName() {
        return schemaName != null ? schemaName + "." + tableName : tableName;
    }
    
    /**
     * Get column definition by name.
     */
    public ColumnDef getColumn(String name) {
        return columns.stream()
            .filter(col -> col.name().equals(name))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Column not found: " + name));
    }
    
    /**
     * Check if table has composite primary key.
     */
    public boolean isCompositeKey() {
        return primaryKey.size() > 1;
    }
}
