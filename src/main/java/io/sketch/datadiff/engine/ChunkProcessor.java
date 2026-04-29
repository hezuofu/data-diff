package io.sketch.datadiff.engine;

import io.sketch.datadiff.core.model.DiffRecord;
import io.sketch.datadiff.core.model.Segment;
import io.sketch.datadiff.core.model.TableInfo;
import io.sketch.datadiff.datasource.dialect.SqlDialect;
import io.sketch.datadiff.datasource.jdbc.JdbcQueryExecutor;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Chunk processor for parallel segment comparison.
 * Processes individual data chunks and returns diff results.
 */
public class ChunkProcessor implements Callable<List<DiffRecord>> {
    
    private final DataSource leftDataSource;
    private final DataSource rightDataSource;
    private final TableInfo leftTable;
    private final TableInfo rightTable;
    private final SqlDialect dialect;
    private final Segment segment;
    private final JdbcQueryExecutor queryExecutor;
    
    public ChunkProcessor(
        DataSource leftDataSource,
        DataSource rightDataSource,
        TableInfo leftTable,
        TableInfo rightTable,
        SqlDialect dialect,
        Segment segment
    ) {
        this.leftDataSource = leftDataSource;
        this.rightDataSource = rightDataSource;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.dialect = dialect;
        this.segment = segment;
        this.queryExecutor = new JdbcQueryExecutor();
    }
    
    @Override
    public List<DiffRecord> call() throws Exception {
        return processChunk();
    }
    
    /**
     * Process a single chunk and return differences.
     */
    private List<DiffRecord> processChunk() throws SQLException {
        // Compute checksums for both sides
        BigInteger leftChecksum = computeChecksum(leftDataSource, leftTable);
        BigInteger rightChecksum = computeChecksum(rightDataSource, rightTable);
        
        // If checksums match, no differences
        if (leftChecksum.equals(rightChecksum)) {
            return List.of();
        }
        
        // Otherwise, extract and compare rows
        return extractDifferences();
    }
    
    /**
     * Compute checksum for this segment.
     */
    private BigInteger computeChecksum(DataSource dataSource, TableInfo table) throws SQLException {
        String pkColumn = table.primaryKey().get(0);
        List<String> columns = table.columns().stream()
            .map(col -> col.name())
            .toList();
        
        String sql = dialect.checksumQuery(
            table.tableName(),
            columns,
            pkColumn,
            segment.rangeStart().longValue(),
            segment.rangeEnd().longValue()
        );
        
        try (var conn = dataSource.getConnection()) {
            var results = queryExecutor.executeQuery(conn, sql);
            if (results.isEmpty()) {
                return BigInteger.ZERO;
            }
            
            Object checksumObj = results.get(0).get("checksum");
            return checksumObj != null ? new BigInteger(checksumObj.toString()) : BigInteger.ZERO;
        }
    }
    
    /**
     * Extract actual differences from this segment.
     */
    private List<DiffRecord> extractDifferences() throws SQLException {
        String pkColumn = leftTable.primaryKey().get(0);
        
        String whereClause = "%s BETWEEN %d AND %d".formatted(
            dialect.quoteIdentifier(pkColumn),
            segment.rangeStart(),
            segment.rangeEnd()
        );
        
        List<Map<String, Object>> leftRows = fetchRows(leftDataSource, leftTable, whereClause);
        List<Map<String, Object>> rightRows = fetchRows(rightDataSource, rightTable, whereClause);
        
        return compareRows(leftRows, rightRows, pkColumn);
    }
    
    private List<Map<String, Object>> fetchRows(
        DataSource dataSource,
        TableInfo table,
        String whereClause
    ) throws SQLException {
        String sql = "SELECT * FROM %s WHERE %s".formatted(
            dialect.quoteIdentifier(table.tableName()),
            whereClause
        );
        
        try (var conn = dataSource.getConnection()) {
            return queryExecutor.executeQuery(conn, sql);
        }
    }
    
    private List<DiffRecord> compareRows(
        List<Map<String, Object>> leftRows,
        List<Map<String, Object>> rightRows,
        String pkColumn
    ) {
        List<DiffRecord> diffs = new java.util.ArrayList<>();
        
        var leftMap = leftRows.stream()
            .collect(java.util.stream.Collectors.toMap(
                row -> row.get(pkColumn),
                row -> row
            ));
        
        var rightMap = rightRows.stream()
            .collect(java.util.stream.Collectors.toMap(
                row -> row.get(pkColumn),
                row -> row
            ));
        
        // Find LEFT_ONLY and MODIFIED
        for (var entry : leftMap.entrySet()) {
            Object pk = entry.getKey();
            Map<String, Object> leftRow = entry.getValue();
            Map<String, Object> rightRow = rightMap.get(pk);
            
            if (rightRow == null) {
                diffs.add(DiffRecord.leftOnly(Map.of(pkColumn, pk), leftRow));
            } else if (!leftRow.equals(rightRow)) {
                List<String> differingCols = findDifferences(leftRow, rightRow);
                diffs.add(DiffRecord.modified(
                    Map.of(pkColumn, pk),
                    leftRow,
                    rightRow,
                    differingCols
                ));
            }
        }
        
        // Find RIGHT_ONLY
        for (var entry : rightMap.entrySet()) {
            Object pk = entry.getKey();
            if (!leftMap.containsKey(pk)) {
                diffs.add(DiffRecord.rightOnly(Map.of(pkColumn, pk), entry.getValue()));
            }
        }
        
        return diffs;
    }
    
    private List<String> findDifferences(Map<String, Object> left, Map<String, Object> right) {
        List<String> diffs = new java.util.ArrayList<>();
        for (String key : left.keySet()) {
            if (!java.util.Objects.equals(left.get(key), right.get(key))) {
                diffs.add(key);
            }
        }
        return diffs;
    }
    
    /**
     * Get the segment being processed.
     */
    public Segment getSegment() {
        return segment;
    }
}
