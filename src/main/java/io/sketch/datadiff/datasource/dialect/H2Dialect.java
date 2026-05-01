package io.sketch.datadiff.datasource.dialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * H2 SQL dialect implementation for testing.
 */
public class H2Dialect extends AbstractSqlDialect {
    
    @Override
    public String hashExpression(List<String> columns) {
        String concatExpr = columns.stream()
            .map(col -> "IFNULL(CAST(" + quoteIdentifier(col) + " AS VARCHAR), '')")
            .collect(Collectors.joining(" || '-' || "));
        // H2 MD5 returns BINARY, we take first 8 bytes and cast to BIGINT
        return "ABS(CAST(SUBSTR(HASH('MD5', CAST(" + concatExpr + " AS VARCHAR), 1), 1, 8) AS BIGINT))";
    }
    
    @Override
    public String getDriverClassName() {
        return "org.h2.Driver";
    }
    
    @Override
    public String getDialectName() {
        return "H2";
    }
}
