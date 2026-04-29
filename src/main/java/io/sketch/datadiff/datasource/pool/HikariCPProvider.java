package io.sketch.datadiff.datasource.pool;

import io.sketch.datadiff.exception.ConnectionException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * HikariCP connection pool provider.
 */
public class HikariCPProvider {
    
    /**
     * Create a HikariCP data source.
     * 
     * @param jdbcUrl JDBC connection URL
     * @param properties connection properties
     * @return HikariCP data source
     */
    public static DataSource createDataSource(String jdbcUrl, Properties properties) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(properties.getProperty("user"));
        config.setPassword(properties.getProperty("password"));
        
        // Default pool configuration
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        
        // Apply additional properties
        properties.forEach((key, value) -> {
            String k = key.toString();
            if (!k.equals("user") && !k.equals("password")) {
                config.addDataSourceProperty(k, value);
            }
        });
        
        try {
            return new HikariDataSource(config);
        } catch (Exception e) {
            throw new ConnectionException("HikariCP", e);
        }
    }
    
    /**
     * Create a HikariCP data source with custom configuration.
     */
    public static DataSource createDataSource(HikariConfig config) {
        try {
            return new HikariDataSource(config);
        } catch (Exception e) {
            throw new ConnectionException("HikariCP", e);
        }
    }
}
