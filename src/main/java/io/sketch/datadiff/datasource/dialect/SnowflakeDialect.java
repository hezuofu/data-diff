package io.sketch.datadiff.datasource.dialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Snowflake SQL dialect implementation.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class SnowflakeDialect extends AbstractSqlDialect {
    
    @Override
    public String quoteIdentifier(String name) {
        return name.toUpperCase();
    }
    
    @Override
    public String hashExpression(List<String> columns) {
        // Snowflake uses SHA2 for hashing
        String concatExpr = columns.stream()
            .map(col -> "IFNULL(" + quoteIdentifier(col) + "::VARCHAR, '')")
            .collect(Collectors.joining(" || '-' || "));
        return "STRTOL(SUBSTRING(SHA2(" + concatExpr + ", 256), 1, 16), 16)";
    }
    
    @Override
    public boolean supportsFullOuterJoin() {
        return true;
    }
    
    @Override
    public String getDriverClassName() {
        return "net.snowflake.client.jdbc.SnowflakeDriver";
    }
    
    @Override
    public String getDialectName() {
        return "Snowflake";
    }
}
