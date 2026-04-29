package io.sketch.datadiff.core.spi;

import java.sql.Connection;
import java.util.Properties;
import io.sketch.datadiff.datasource.dialect.SqlDialect;

/**
 * Service Provider Interface for data source connections.
 * Implementations should be registered via META-INF/services/com.datadiff.core.spi.DataSourceProvider
 *
 * @author lanxia39@163.com
 *
 * 
 */
public interface DataSourceProvider {
    
    /**
     * Check if this provider supports the given JDBC URL.
     * 
     * @param jdbcUrl JDBC URL to check
     * @return true if this provider can handle the URL
     */
    boolean supports(String jdbcUrl);
    
    /**
     * Create a connection to the data source.
     * 
     * @param jdbcUrl JDBC connection URL
     * @param properties connection properties (username, password, etc.)
     * @return database connection
     */
    Connection getConnection(String jdbcUrl, Properties properties);
    
    /**
     * Get the SQL dialect for this data source.
     * 
     * @return SQL dialect implementation
     */
    SqlDialect getDialect();
    
    /**
     * Get the provider name.
     * 
     * @return provider name identifier
     */
    String getProviderName();
}
