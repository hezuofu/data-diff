package io.sketch.datadiff.datasource.dialect;

import io.sketch.datadiff.exception.DataDiffException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolver for SQL dialects based on JDBC URL.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
public class DialectResolver {
    
    private static final Map<String, SqlDialect> DIALECT_REGISTRY = new ConcurrentHashMap<>();
    
    static {
        // Register built-in dialects
        registerDialect("jdbc:mysql:", new MySqlDialect());
        registerDialect("jdbc:mariadb:", new MySqlDialect());
        registerDialect("jdbc:postgresql:", new PostgreSqlDialect());
        registerDialect("jdbc:snowflake:", new SnowflakeDialect());
    }
    
    /**
     * Register a dialect for a JDBC URL prefix.
     * 
     * @param urlPrefix JDBC URL prefix (e.g., "jdbc:mysql:")
     * @param dialect SQL dialect implementation
     */
    public static void registerDialect(String urlPrefix, SqlDialect dialect) {
        DIALECT_REGISTRY.put(urlPrefix.toLowerCase(), dialect);
    }
    
    /**
     * Resolve dialect for a JDBC URL.
     * 
     * @param jdbcUrl JDBC connection URL
     * @return SQL dialect
     * @throws DataDiffException if no dialect is found for the URL
     */
    public static SqlDialect resolve(String jdbcUrl) {
        String url = jdbcUrl.toLowerCase();
        
        for (Map.Entry<String, SqlDialect> entry : DIALECT_REGISTRY.entrySet()) {
            if (url.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        
        throw new DataDiffException("No SQL dialect found for JDBC URL: " + jdbcUrl);
    }
    
    /**
     * Get all registered dialects.
     * 
     * @return unmodifiable map of URL prefixes to dialects
     */
    public static Map<String, SqlDialect> getRegisteredDialects() {
        return Map.copyOf(DIALECT_REGISTRY);
    }
}
