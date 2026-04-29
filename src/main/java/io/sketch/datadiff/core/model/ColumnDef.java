package io.sketch.datadiff.core.model;

/**
 * Column definition record.
 * 
 * @param name column name
 * @param dataType SQL data type (e.g., VARCHAR, INTEGER, TIMESTAMP)
 * @param nullable whether column allows NULL values
 *
 * @author lanxia39@163.com
 */
public record ColumnDef(
    String name,
    String dataType,
    boolean nullable
) {
    public ColumnDef {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Column name cannot be null or blank");
        }
        if (dataType == null || dataType.isBlank()) {
            throw new IllegalArgumentException("Data type cannot be null or blank");
        }
    }
    
    /**
     * Check if this is a numeric type.
     */
    public boolean isNumeric() {
        return dataType.matches("(?i)(tinyint|smallint|mediumint|int|integer|bigint|decimal|numeric|float|double|real)");
    }
    
    /**
     * Check if this is a string type.
     */
    public boolean isString() {
        return dataType.matches("(?i)(char|varchar|text|longtext|mediumtext|tinytext)");
    }
    
    /**
     * Check if this is a date/time type.
     */
    public boolean isDateTime() {
        return dataType.matches("(?i)(date|time|datetime|timestamp|year)");
    }
}
