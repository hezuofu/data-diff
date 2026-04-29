package io.sketch.datadiff.engine;

import io.sketch.datadiff.exception.DataDiffException;
import io.sketch.datadiff.core.model.DiffRecord;
import io.sketch.datadiff.core.model.Segment;
import io.sketch.datadiff.core.model.TableInfo;
import io.sketch.datadiff.datasource.dialect.SqlDialect;
import io.sketch.datadiff.datasource.jdbc.JdbcQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Recursive bisector for locating differing rows through binary search.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
public class RecursiveBisector {
    
    private static final Logger log = LoggerFactory.getLogger(RecursiveBisector.class);
    
    private final DataSource leftDataSource;
    private final DataSource rightDataSource;
    private final TableInfo leftTable;
    private final TableInfo rightTable;
    private final SqlDialect dialect;
    private final JdbcQueryExecutor queryExecutor;
    private final int maxDepth;
    
    private int currentMaxDepth = 0;
    private int iterations = 0;
    
    public RecursiveBisector(
        DataSource leftDataSource,
        DataSource rightDataSource,
        TableInfo leftTable,
        TableInfo rightTable,
        SqlDialect dialect,
        int maxDepth
    ) {
        this.leftDataSource = leftDataSource;
        this.rightDataSource = rightDataSource;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.dialect = dialect;
        this.queryExecutor = new JdbcQueryExecutor();
        this.maxDepth = maxDepth;
    }
    
    /**
     * Bisect a mismatched segment to find differing rows.
     */
    public List<DiffRecord> bisect(Segment segment) {
        iterations++;
        currentMaxDepth = Math.max(currentMaxDepth, segment.depth());
        
        try {
            return bisectRecursive(segment, 0);
        } catch (SQLException e) {
            throw new DataDiffException("Bisection failed", e);
        }
    }
    
    private List<DiffRecord> bisectRecursive(Segment segment, int depth) throws SQLException {
        List<DiffRecord> diffs = new ArrayList<>();
        
        // Base case: reached max depth or segment is small enough
        if (depth >= maxDepth || segment.count() <= 10) {
            return extractDiffRows(segment);
        }
        
        // Split segment in half
        Segment[] halves = segment.bisect();
        Segment leftHalf = halves[0];
        Segment rightHalf = halves[1];
        
        // Check each half for mismatches
        for (Segment half : halves) {
            if (hasMismatch(half)) {
                diffs.addAll(bisectRecursive(half, depth + 1));
            }
        }
        
        return diffs;
    }
    
    /**
     * Check if a segment has mismatched checksums.
     */
    private boolean hasMismatch(Segment segment) throws SQLException {
        BigInteger leftChecksum = computeChecksum(leftDataSource, leftTable, segment);
        BigInteger rightChecksum = computeChecksum(rightDataSource, rightTable, segment);
        
        boolean mismatch = !leftChecksum.equals(rightChecksum);
        if (mismatch) {
            log.debug("Mismatch found in segment {} at depth {}", segment, segment.depth());
        }
        
        return mismatch;
    }
    
    /**
     * Compute checksum for a segment.
     */
    private BigInteger computeChecksum(DataSource dataSource, TableInfo table, Segment segment) 
        throws SQLException {
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
            if (checksumObj == null) {
                return BigInteger.ZERO;
            }
            
            return new BigInteger(checksumObj.toString());
        }
    }
    
    /**
     * Extract actual differing rows from a small segment.
     */
    private List<DiffRecord> extractDiffRows(Segment segment) throws SQLException {
        String pkColumn = leftTable.primaryKey().get(0);
        List<String> compareColumns = leftTable.columns().stream()
            .map(col -> col.name())
            .filter(col -> !col.equals(pkColumn))
            .toList();
        
        // Fetch rows from both tables
        String whereClause = "%s BETWEEN %d AND %d".formatted(
            dialect.quoteIdentifier(pkColumn),
            segment.rangeStart(),
            segment.rangeEnd()
        );
        
        List<Map<String, Object>> leftRows = fetchRows(leftDataSource, leftTable, whereClause);
        List<Map<String, Object>> rightRows = fetchRows(rightDataSource, rightTable, whereClause);
        
        // Compare rows
        return compareRows(leftRows, rightRows, pkColumn, compareColumns);
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
        String pkColumn,
        List<String> compareColumns
    ) {
        List<DiffRecord> diffs = new ArrayList<>();
        
        // Index rows by primary key
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
                diffs.add(DiffRecord.leftOnly(
                    Map.of(pkColumn, pk),
                    leftRow
                ));
            } else {
                // Check for differences
                List<String> differingCols = new ArrayList<>();
                for (String col : compareColumns) {
                    Object leftVal = leftRow.get(col);
                    Object rightVal = rightRow.get(col);
                    if (!java.util.Objects.equals(leftVal, rightVal)) {
                        differingCols.add(col);
                    }
                }
                
                if (!differingCols.isEmpty()) {
                    diffs.add(DiffRecord.modified(
                        Map.of(pkColumn, pk),
                        leftRow,
                        rightRow,
                        differingCols
                    ));
                }
            }
        }
        
        // Find RIGHT_ONLY
        for (var entry : rightMap.entrySet()) {
            Object pk = entry.getKey();
            if (!leftMap.containsKey(pk)) {
                diffs.add(DiffRecord.rightOnly(
                    Map.of(pkColumn, pk),
                    entry.getValue()
                ));
            }
        }
        
        return diffs;
    }
    
    /**
     * Get the maximum bisection depth reached.
     */
    public int getCurrentMaxDepth() {
        return currentMaxDepth;
    }
    
    /**
     * Get the number of bisection iterations.
     */
    public int getIterations() {
        return iterations;
    }
}
