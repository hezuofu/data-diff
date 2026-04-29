package io.sketch.datadiff.datasource.dialect;

import java.util.List;

/**
 * SQL dialect interface for database-specific SQL generation.
 */
public interface SqlDialect {
    
    /**
     * Quote an identifier (table name, column name) according to database rules.
     * 
     * @param name identifier to quote
     * @return quoted identifier
     */
    String quoteIdentifier(String name);
    
    /**
     * Generate hash expression for the given columns.
     * 
     * @param columns column names to hash
     * @return SQL expression that computes hash
     */
    String hashExpression(List<String> columns);
    
    /**
     * Generate type cast expression.
     * 
     * @param column column name
     * @param dataType target data type
     * @return SQL expression for type casting
     */
    String typeCast(String column, String dataType);
    
    /**
     * Generate LIMIT clause.
     * 
     * @param limit number of rows to limit
     * @return SQL LIMIT clause
     */
    String limitClause(int limit);
    
    /**
     * Generate query to get MIN/MAX of a column.
     * 
     * @param tableName table name
     * @param columnName column name
     * @return SQL query
     */
    String minMaxQuery(String tableName, String columnName);
    
    /**
     * Generate query to count rows with optional WHERE clause.
     * 
     * @param tableName table name
     * @param whereClause WHERE clause (may be null)
     * @return SQL query
     */
    String countQuery(String tableName, String whereClause);
    
    /**
     * Generate query to compute checksum for a segment.
     * 
     * @param tableName table name
     * @param columns columns to include in hash
     * @param keyColumn primary key column
     * @param rangeStart range start value
     * @param rangeEnd range end value
     * @return SQL query
     */
    String checksumQuery(
        String tableName,
        List<String> columns,
        String keyColumn,
        long rangeStart,
        long rangeEnd
    );
    
    /**
     * Get the JDBC driver class name.
     * 
     * @return driver class name
     */
    String getDriverClassName();
    
    /**
     * Get the dialect name.
     * 
     * @return dialect name
     */
    String getDialectName();
    
    /**
     * Check if this dialect supports FULL OUTER JOIN.
     * 
     * @return true if FULL OUTER JOIN is supported
     */
    default boolean supportsFullOuterJoin() {
        return true;
    }
}
