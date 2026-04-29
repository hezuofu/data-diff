package io.sketch.datadiff.stream;

import io.sketch.datadiff.core.model.TableInfo;
import io.sketch.datadiff.datasource.dialect.SqlDialect;
import io.sketch.datadiff.datasource.jdbc.JdbcQueryExecutor;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Partition reader for streaming large table reads in chunks.
 * Avoids loading entire table into memory.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class PartitionReader implements AutoCloseable {
    
    private final DataSource dataSource;
    private final TableInfo table;
    private final SqlDialect dialect;
    private final JdbcQueryExecutor queryExecutor;
    private final int fetchSize;
    
    public PartitionReader(DataSource dataSource, TableInfo table, SqlDialect dialect, int fetchSize) {
        this.dataSource = dataSource;
        this.table = table;
        this.dialect = dialect;
        this.queryExecutor = new JdbcQueryExecutor();
        this.fetchSize = fetchSize;
    }
    
    public PartitionReader(DataSource dataSource, TableInfo table, SqlDialect dialect) {
        this(dataSource, table, dialect, 1000);
    }
    
    /**
     * Stream all rows from table in partitions.
     */
    public Stream<Map<String, Object>> streamAll() throws SQLException {
        String pkColumn = table.primaryKey().get(0);
        BigInteger minKey = getMinKey(pkColumn);
        BigInteger maxKey = getMaxKey(pkColumn);
        
        return streamRange(minKey, maxKey);
    }
    
    /**
     * Stream rows in a specific range.
     */
    public Stream<Map<String, Object>> streamRange(BigInteger startKey, BigInteger endKey) throws SQLException {
        String pkColumn = table.primaryKey().get(0);
        String sql = buildRangeQuery(pkColumn, startKey, endKey);
        
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(new RowIterator(sql), Spliterator.ORDERED),
            false
        );
    }
    
    /**
     * Stream rows with custom WHERE clause.
     */
    public Stream<Map<String, Object>> streamWhere(String whereClause) throws SQLException {
        String sql = "SELECT * FROM %s WHERE %s ORDER BY %s".formatted(
            dialect.quoteIdentifier(table.tableName()),
            whereClause,
            dialect.quoteIdentifier(table.primaryKey().get(0))
        );
        
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(new RowIterator(sql), Spliterator.ORDERED),
            false
        );
    }
    
    /**
     * Build range query SQL.
     */
    private String buildRangeQuery(String pkColumn, BigInteger startKey, BigInteger endKey) {
        return "SELECT * FROM %s WHERE %s BETWEEN %d AND %d ORDER BY %s".formatted(
            dialect.quoteIdentifier(table.tableName()),
            dialect.quoteIdentifier(pkColumn),
            startKey,
            endKey,
            dialect.quoteIdentifier(pkColumn)
        );
    }
    
    /**
     * Get minimum key value.
     */
    private BigInteger getMinKey(String pkColumn) throws SQLException {
        String sql = "SELECT MIN(%s) as min_val FROM %s".formatted(
            dialect.quoteIdentifier(pkColumn),
            dialect.quoteIdentifier(table.tableName())
        );
        
        try (var conn = dataSource.getConnection()) {
            var results = queryExecutor.executeQuery(conn, sql);
            Object val = results.get(0).get("min_val");
            return val != null ? new BigInteger(val.toString()) : BigInteger.ZERO;
        }
    }
    
    /**
     * Get maximum key value.
     */
    private BigInteger getMaxKey(String pkColumn) throws SQLException {
        String sql = "SELECT MAX(%s) as max_val FROM %s".formatted(
            dialect.quoteIdentifier(pkColumn),
            dialect.quoteIdentifier(table.tableName())
        );
        
        try (var conn = dataSource.getConnection()) {
            var results = queryExecutor.executeQuery(conn, sql);
            Object val = results.get(0).get("max_val");
            return val != null ? new BigInteger(val.toString()) : BigInteger.ZERO;
        }
    }
    
    /**
     * Iterator for streaming rows.
     */
    private class RowIterator implements Iterator<Map<String, Object>> {
        private final String sql;
        private List<Map<String, Object>> buffer;
        private int index = 0;
        private boolean exhausted = false;
        private BigInteger lastKey = BigInteger.ZERO;
        
        RowIterator(String sql) {
            this.sql = sql;
            loadNextBatch();
        }
        
        private void loadNextBatch() {
            try {
                if (buffer != null && buffer.size() < fetchSize) {
                    exhausted = true;
                    return;
                }
                
                String batchSql = sql + " LIMIT " + fetchSize + " OFFSET " + index;
                try (var conn = dataSource.getConnection()) {
                    buffer = queryExecutor.executeQuery(conn, batchSql);
                }
                
                index += buffer.size();
                if (buffer.isEmpty()) {
                    exhausted = true;
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed to load next batch", e);
            }
        }
        
        @Override
        public boolean hasNext() {
            if (buffer == null || index >= buffer.size()) {
                loadNextBatch();
            }
            return !exhausted && buffer != null && index < buffer.size();
        }
        
        @Override
        public Map<String, Object> next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return buffer.get(index++);
        }
    }
    
    @Override
    public void close() {
        // Resources are managed by try-with-resources in queries
    }
}
