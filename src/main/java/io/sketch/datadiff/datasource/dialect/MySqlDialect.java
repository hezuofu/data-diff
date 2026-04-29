package io.sketch.datadiff.datasource.dialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * MySQL SQL dialect implementation.
 */
public class MySqlDialect extends AbstractSqlDialect {
    
    @Override
    public String quoteIdentifier(String name) {
        return "`" + name + "`";
    }
    
    @Override
    public String hashExpression(List<String> columns) {
        // MySQL uses MD5 for hashing
        String concatExpr = columns.stream()
            .map(col -> "IFNULL(" + quoteIdentifier(col) + ", '')")
            .collect(Collectors.joining(", '-', "));
        return "CONV(SUBSTRING(MD5(CONCAT(" + concatExpr + ")), 1, 16), 16, 10)";
    }
    
    @Override
    public String getDriverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }
    
    @Override
    public String getDialectName() {
        return "MySQL";
    }
}
