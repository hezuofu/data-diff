package io.sketch.datadiff.core.strategy;

import io.sketch.datadiff.core.model.ColumnDef;
import io.sketch.datadiff.datasource.dialect.SqlDialect;

/**
 * Strategy interface for ordering rows during comparison.
 */
@FunctionalInterface
public interface OrderingStrategy {
    
    /**
     * Generate ORDER BY clause for the given columns.
     * 
     * @param columns columns to order by
     * @param dialect SQL dialect
     * @return ORDER BY SQL clause
     */
    String generateOrderBy(java.util.List<ColumnDef> columns, SqlDialect dialect);
    
    /**
     * Default implementation that orders by column names ascending.
     */
    OrderingStrategy ASCENDING = (columns, dialect) -> {
        String columnList = columns.stream()
            .map(col -> dialect.quoteIdentifier(col.name()))
            .collect(java.util.stream.Collectors.joining(", "));
        return "ORDER BY " + columnList;
    };
}
