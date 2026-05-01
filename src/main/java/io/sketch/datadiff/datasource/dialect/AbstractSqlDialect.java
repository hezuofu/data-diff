package io.sketch.datadiff.datasource.dialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract base class for SQL dialects providing common implementations.
 */
public abstract class AbstractSqlDialect implements SqlDialect {
    
    @Override
    public String quoteIdentifier(String name) {
        return "\"" + name + "\"";
    }
    
    @Override
    public String typeCast(String column, String dataType) {
        return "CAST(" + quoteIdentifier(column) + " AS " + dataType + ")";
    }
    
    @Override
    public String limitClause(int limit) {
        return "LIMIT " + limit;
    }
    
    @Override
    public String minMaxQuery(String tableName, String columnName) {
        return "SELECT MIN(%s) as \"min_val\", MAX(%s) as \"max_val\" FROM %s".formatted(
            quoteIdentifier(columnName),
            quoteIdentifier(columnName),
            quoteIdentifier(tableName)
        );
    }
    
    @Override
    public String countQuery(String tableName, String whereClause) {
        String sql = "SELECT COUNT(*) FROM " + quoteIdentifier(tableName);
        if (whereClause != null && !whereClause.isBlank()) {
            sql += " WHERE " + whereClause;
        }
        return sql;
    }
    
    @Override
    public String checksumQuery(
        String tableName,
        List<String> columns,
        String keyColumn,
        long rangeStart,
        long rangeEnd
    ) {
        String hashExpr = hashExpression(columns);
        return """
            SELECT 
              SUM(%s) as \"checksum\",
              COUNT(*) as \"row_count\"
            FROM %s
            WHERE %s BETWEEN %d AND %d
            """.formatted(
            hashExpr,
            quoteIdentifier(tableName),
            quoteIdentifier(keyColumn),
            rangeStart,
            rangeEnd
        );
    }
    
    /**
     * Generate comma-separated quoted column names.
     */
    protected String quoteColumns(List<String> columns) {
        return columns.stream()
            .map(this::quoteIdentifier)
            .collect(Collectors.joining(", "));
    }
}
