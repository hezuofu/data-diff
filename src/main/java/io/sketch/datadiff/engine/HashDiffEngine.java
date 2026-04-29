package io.sketch.datadiff.engine;

import io.sketch.datadiff.core.model.*;
import io.sketch.datadiff.exception.DataDiffException;
import io.sketch.datadiff.core.model.*;
import io.sketch.datadiff.core.strategy.ComparisonStrategy;
import io.sketch.datadiff.datasource.dialect.DialectResolver;
import io.sketch.datadiff.datasource.dialect.SqlDialect;
import io.sketch.datadiff.datasource.jdbc.JdbcQueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Hash-based diff engine using checksums and recursive bisection.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
public class HashDiffEngine implements ComparisonStrategy {
    
    private static final Logger log = LoggerFactory.getLogger(HashDiffEngine.class);
    
    private final SegmentSplitter segmentSplitter;
    private final JdbcQueryExecutor queryExecutor;
    
    public HashDiffEngine() {
        this.segmentSplitter = new SegmentSplitter();
        this.queryExecutor = new JdbcQueryExecutor();
    }
    
    @Override
    public DiffResult compare(
        DataSource leftDataSource,
        DataSource rightDataSource,
        TableInfo leftTable,
        TableInfo rightTable,
        CompareOptions options
    ) {
        Instant start = Instant.now();
        log.info("Starting HashDiff comparison: {} vs {}", 
            leftTable.getFullName(), rightTable.getFullName());
        
        try {
            // Get row counts and key ranges
            long leftCount = getRowCount(leftDataSource, leftTable);
            long rightCount = getRowCount(rightDataSource, rightTable);
            
            BigInteger minKey = getMinKey(leftDataSource, leftTable);
            BigInteger maxKey = getMaxKey(rightDataSource, rightTable);
            
            // Split into segments
            List<Segment> segments = segmentSplitter.split(
                minKey, maxKey, Math.max(leftCount, rightCount), options.getSegmentSize()
            );
            
            log.info("Split data into {} segments", segments.size());
            
            // Process segments in parallel using virtual threads
            List<DiffRecord> allDiffs = processSegments(
                leftDataSource, rightDataSource,
                leftTable, rightTable,
                segments, options
            );
            
            Instant end = Instant.now();
            Duration duration = Duration.between(start, end);
            
            DiffResult.Statistics stats = new DiffResult.Statistics(
                leftCount, rightCount,
                segments.size(), 0, 0, 0
            );
            
            log.info("HashDiff completed in {}: {} differences found", duration, allDiffs.size());
            
            return new DiffResult(allDiffs, stats, duration);
            
        } catch (SQLException e) {
            throw new DataDiffException("HashDiff comparison failed", e);
        }
    }
    
    private List<DiffRecord> processSegments(
        DataSource leftDataSource,
        DataSource rightDataSource,
        TableInfo leftTable,
        TableInfo rightTable,
        List<Segment> segments,
        CompareOptions options
    ) throws SQLException {
        List<DiffRecord> allDiffs = new ArrayList<>();
        
        // Use fixed thread pool for parallel processing (Java 17)
        ExecutorService executor = Executors.newFixedThreadPool(options.getParallelism());
        try {
            List<Future<List<DiffRecord>>> futures = new ArrayList<>();
            
            for (Segment segment : segments) {
                futures.add(executor.submit(() -> 
                    processSegment(leftDataSource, rightDataSource, 
                        leftTable, rightTable, segment)
                ));
            }
            
            // Collect results
            for (Future<List<DiffRecord>> future : futures) {
                try {
                    List<DiffRecord> segmentDiffs = future.get();
                    allDiffs.addAll(segmentDiffs);
                } catch (Exception e) {
                    throw new DataDiffException("Segment processing failed", e);
                }
            }
        } finally {
            executor.shutdown();
        }
        
        return allDiffs;
    }
    
    private List<DiffRecord> processSegment(
        DataSource leftDataSource,
        DataSource rightDataSource,
        TableInfo leftTable,
        TableInfo rightTable,
        Segment segment
    ) throws SQLException {
        // Compute checksums for both sides
        BigInteger leftChecksum = computeSegmentChecksum(leftDataSource, leftTable, segment);
        BigInteger rightChecksum = computeSegmentChecksum(rightDataSource, rightTable, segment);
        
        // If checksums match, no differences in this segment
        if (leftChecksum.equals(rightChecksum)) {
            log.debug("Segment {} checksums match", segment);
            return List.of();
        }
        
        log.debug("Segment {} checksums mismatch, bisecting...", segment);
        
        // Bisect to find differences
        SqlDialect dialect = DialectResolver.resolve("jdbc:mysql:");
        RecursiveBisector bisector = new RecursiveBisector(
            leftDataSource, rightDataSource,
            leftTable, rightTable,
            dialect, 32
        );
        
        return bisector.bisect(segment);
    }
    
    private BigInteger computeSegmentChecksum(
        DataSource dataSource,
        TableInfo table,
        Segment segment
    ) throws SQLException {
        String pkColumn = table.primaryKey().get(0);
        List<String> columns = new ArrayList<>();
        for (var col : table.columns()) {
            columns.add(col.name());
        }
        
        SqlDialect dialect = DialectResolver.resolve("jdbc:mysql:");
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
    
    private long getRowCount(DataSource dataSource, TableInfo table) throws SQLException {
        SqlDialect dialect = DialectResolver.resolve("jdbc:mysql:");
        String sql = dialect.countQuery(table.tableName(), null);
        
        try (var conn = dataSource.getConnection()) {
            return queryExecutor.executeScalar(conn, sql, Long.class);
        }
    }
    
    private BigInteger getMinKey(DataSource dataSource, TableInfo table) throws SQLException {
        String pkColumn = table.primaryKey().get(0);
        SqlDialect dialect = DialectResolver.resolve("jdbc:mysql:");
        String sql = dialect.minMaxQuery(table.tableName(), pkColumn);
        
        try (var conn = dataSource.getConnection()) {
            var results = queryExecutor.executeQuery(conn, sql);
            Object minVal = results.get(0).get("min_val");
            return minVal != null ? new BigInteger(minVal.toString()) : BigInteger.ZERO;
        }
    }
    
    private BigInteger getMaxKey(DataSource dataSource, TableInfo table) throws SQLException {
        String pkColumn = table.primaryKey().get(0);
        SqlDialect dialect = DialectResolver.resolve("jdbc:mysql:");
        String sql = dialect.minMaxQuery(table.tableName(), pkColumn);
        
        try (var conn = dataSource.getConnection()) {
            var results = queryExecutor.executeQuery(conn, sql);
            Object maxVal = results.get(0).get("max_val");
            return maxVal != null ? new BigInteger(maxVal.toString()) : BigInteger.ZERO;
        }
    }
    
    @Override
    public String getStrategyName() {
        return "HashDiff";
    }
}
