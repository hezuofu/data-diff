package io.sketch.datadiff.engine;

import io.sketch.datadiff.core.model.CompareOptions;
import io.sketch.datadiff.core.model.DiffRecord;
import io.sketch.datadiff.core.model.TableInfo;
import io.sketch.datadiff.datasource.dialect.SqlDialect;
import io.sketch.datadiff.datasource.jdbc.JdbcQueryExecutor;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Streaming comparator that processes large tables in chunks with memory control.
 * Uses streaming approach to avoid loading entire tables into memory.
 */
public class StreamingComparator {
    
    private final DataSource leftDataSource;
    private final DataSource rightDataSource;
    private final TableInfo leftTable;
    private final TableInfo rightTable;
    private final SqlDialect dialect;
    private final CompareOptions options;
    private final JdbcQueryExecutor queryExecutor;
    
    public StreamingComparator(
        DataSource leftDataSource,
        DataSource rightDataSource,
        TableInfo leftTable,
        TableInfo rightTable,
        SqlDialect dialect,
        CompareOptions options
    ) {
        this.leftDataSource = leftDataSource;
        this.rightDataSource = rightDataSource;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.dialect = dialect;
        this.options = options;
        this.queryExecutor = new JdbcQueryExecutor();
    }
    
    /**
     * Compare tables using streaming approach with bounded memory.
     */
    public List<DiffRecord> compare() throws SQLException {
        String pkColumn = leftTable.primaryKey().get(0);
        
        // Get key range
        BigInteger minKey = getMinKey(leftDataSource, leftTable, pkColumn);
        BigInteger maxKey = getMaxKey(rightDataSource, rightTable, pkColumn);
        
        // Process in streaming chunks
        List<DiffRecord> allDiffs = new ArrayList<>();
        BigInteger currentKey = minKey;
        
        while (currentKey.compareTo(maxKey) <= 0) {
            BigInteger nextKey = currentKey.add(BigInteger.valueOf(options.getSegmentSize()));
            
            List<DiffRecord> chunkDiffs = compareChunk(currentKey, nextKey.subtract(BigInteger.ONE));
            allDiffs.addAll(chunkDiffs);
            
            currentKey = nextKey;
        }
        
        return allDiffs;
    }
    
    /**
     * Compare a single chunk of data.
     */
    private List<DiffRecord> compareChunk(BigInteger startKey, BigInteger endKey) throws SQLException {
        String pkColumn = leftTable.primaryKey().get(0);
        
        String whereClause = "%s BETWEEN %d AND %d".formatted(
            dialect.quoteIdentifier(pkColumn),
            startKey,
            endKey
        );
        
        // Stream rows from both tables
        try (var leftConn = leftDataSource.getConnection();
             var rightConn = rightDataSource.getConnection()) {
            
            String sql = "SELECT * FROM %s WHERE %s ORDER BY %s".formatted(
                dialect.quoteIdentifier(leftTable.tableName()),
                whereClause,
                dialect.quoteIdentifier(pkColumn)
            );
            
            List<Map<String, Object>> leftRows = queryExecutor.executeQuery(leftConn, sql);
            List<Map<String, Object>> rightRows = queryExecutor.executeQuery(rightConn, 
                sql.replace(leftTable.tableName(), rightTable.tableName()));
            
            return compareRowSets(leftRows, rightRows, pkColumn);
        }
    }
    
    /**
     * Compare two sets of rows.
     */
    private List<DiffRecord> compareRowSets(
        List<Map<String, Object>> leftRows,
        List<Map<String, Object>> rightRows,
        String pkColumn
    ) {
        List<DiffRecord> diffs = new ArrayList<>();
        
        var leftMap = indexByPk(leftRows, pkColumn);
        var rightMap = indexByPk(rightRows, pkColumn);
        
        // Check left rows
        for (Map.Entry<Object, Map<String, Object>> entry : leftMap.entrySet()) {
            Object pk = entry.getKey();
            Map<String, Object> leftRow = entry.getValue();
            Map<String, Object> rightRow = rightMap.get(pk);
            
            if (rightRow == null) {
                diffs.add(DiffRecord.leftOnly(Map.of(pkColumn, pk), leftRow));
            } else {
                List<String> diffCols = findDifferences(leftRow, rightRow);
                if (!diffCols.isEmpty()) {
                    diffs.add(DiffRecord.modified(
                        Map.of(pkColumn, pk),
                        leftRow,
                        rightRow,
                        diffCols
                    ));
                }
            }
        }
        
        // Check for right-only rows
        for (Map.Entry<Object, Map<String, Object>> entry : rightMap.entrySet()) {
            Object pk = entry.getKey();
            if (!leftMap.containsKey(pk)) {
                diffs.add(DiffRecord.rightOnly(Map.of(pkColumn, pk), entry.getValue()));
            }
        }
        
        return diffs;
    }
    
    /**
     * Index rows by primary key.
     */
    private Map<Object, Map<String, Object>> indexByPk(
        List<Map<String, Object>> rows,
        String pkColumn
    ) {
        Map<Object, Map<String, Object>> index = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            Object pk = row.get(pkColumn);
            index.put(pk, row);
        }
        return index;
    }
    
    /**
     * Find differing columns between two rows.
     */
    private List<String> findDifferences(Map<String, Object> left, Map<String, Object> right) {
        List<String> diffs = new ArrayList<>();
        for (String col : left.keySet()) {
            if (!java.util.Objects.equals(left.get(col), right.get(col))) {
                diffs.add(col);
            }
        }
        return diffs;
    }
    
    /**
     * Get minimum key value.
     */
    private BigInteger getMinKey(DataSource ds, TableInfo table, String pkColumn) throws SQLException {
        String sql = "SELECT MIN(%s) as min_val FROM %s".formatted(
            dialect.quoteIdentifier(pkColumn),
            dialect.quoteIdentifier(table.tableName())
        );
        
        try (var conn = ds.getConnection()) {
            var results = queryExecutor.executeQuery(conn, sql);
            Object val = results.get(0).get("min_val");
            return val != null ? new BigInteger(val.toString()) : BigInteger.ZERO;
        }
    }
    
    /**
     * Get maximum key value.
     */
    private BigInteger getMaxKey(DataSource ds, TableInfo table, String pkColumn) throws SQLException {
        String sql = "SELECT MAX(%s) as max_val FROM %s".formatted(
            dialect.quoteIdentifier(pkColumn),
            dialect.quoteIdentifier(table.tableName())
        );
        
        try (var conn = ds.getConnection()) {
            var results = queryExecutor.executeQuery(conn, sql);
            Object val = results.get(0).get("max_val");
            return val != null ? new BigInteger(val.toString()) : BigInteger.ZERO;
        }
    }
    
    /**
     * Compare tables in parallel using streaming approach.
     */
    public List<DiffRecord> compareParallel() throws Exception {
        String pkColumn = leftTable.primaryKey().get(0);
        
        BigInteger minKey = getMinKey(leftDataSource, leftTable, pkColumn);
        BigInteger maxKey = getMaxKey(rightDataSource, rightTable, pkColumn);
        
        BigInteger range = maxKey.subtract(minKey).add(BigInteger.ONE);
        long chunkSize = options.getSegmentSize();
        int numChunks = (int) Math.ceil(range.doubleValue() / chunkSize);
        
        List<Future<List<DiffRecord>>> futures = new ArrayList<>();
        
        ExecutorService executor = Executors.newFixedThreadPool(options.getParallelism());
        try {
            BigInteger currentKey = minKey;
            
            for (int i = 0; i < numChunks; i++) {
                BigInteger startKey = currentKey;
                BigInteger endKey = currentKey.add(BigInteger.valueOf(chunkSize)).subtract(BigInteger.ONE);
                
                futures.add(executor.submit(() -> compareChunk(startKey, endKey)));
                
                currentKey = endKey.add(BigInteger.ONE);
            }
            
            List<DiffRecord> allDiffs = new ArrayList<>();
            for (Future<List<DiffRecord>> future : futures) {
                allDiffs.addAll(future.get());
            }
            
            return allDiffs;
        } finally {
            executor.shutdown();
        }
    }
}
