package io.sketch.datadiff.datasource.dialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * PostgreSQL SQL dialect implementation.
 */
public class PostgreSqlDialect extends AbstractSqlDialect {
    
    @Override
    public String hashExpression(List<String> columns) {
        // PostgreSQL uses MD5 for hashing
        String concatExpr = columns.stream()
            .map(col -> "COALESCE(" + quoteIdentifier(col) + "::text, '')")
            .collect(Collectors.joining(" || '-' || "));
        return ("hashtext(" + concatExpr + ")::bigint");
    }
    
    @Override
    public String limitClause(int limit) {
        return "LIMIT " + limit;
    }
    
    @Override
    public String getDriverClassName() {
        return "org.postgresql.Driver";
    }
    
    @Override
    public String getDialectName() {
        return "PostgreSQL";
    }
}
