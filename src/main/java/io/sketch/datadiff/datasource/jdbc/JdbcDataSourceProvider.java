package io.sketch.datadiff.datasource.jdbc;

import io.sketch.datadiff.exception.ConnectionException;
import io.sketch.datadiff.core.spi.DataSourceProvider;
import io.sketch.datadiff.datasource.dialect.DialectResolver;
import io.sketch.datadiff.datasource.dialect.SqlDialect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * JDBC data source provider implementation.
 */
public class JdbcDataSourceProvider implements DataSourceProvider {
    
    @Override
    public boolean supports(String jdbcUrl) {
        return jdbcUrl != null && jdbcUrl.toLowerCase().startsWith("jdbc:");
    }
    
    @Override
    public Connection getConnection(String jdbcUrl, Properties properties) {
        try {
            return DriverManager.getConnection(jdbcUrl, properties);
        } catch (SQLException e) {
            throw new ConnectionException("JDBC", e);
        }
    }
    
    @Override
    public SqlDialect getDialect() {
        throw new UnsupportedOperationException(
            "Use getDialect(jdbcUrl) instead"
        );
    }
    
    /**
     * Get dialect for a specific JDBC URL.
     */
    public SqlDialect getDialect(String jdbcUrl) {
        return DialectResolver.resolve(jdbcUrl);
    }
    
    @Override
    public String getProviderName() {
        return "JDBC";
    }
}
