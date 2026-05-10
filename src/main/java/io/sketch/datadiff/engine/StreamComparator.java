package io.sketch.datadiff.engine;

import io.sketch.datadiff.core.model.*;
import io.sketch.datadiff.core.strategy.ComparisonStrategy;
import io.sketch.datadiff.datasource.dialect.DialectResolver;
import io.sketch.datadiff.datasource.dialect.SqlDialect;
import io.sketch.datadiff.datasource.jdbc.JdbcQueryExecutor;
import io.sketch.datadiff.exception.DataDiffException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Streaming comparison strategy — walks through key ranges in chunks,
 * comparing rows directly without checksum pre-filtering.
 * Best for moderate tables or when many differences are expected.
 *
 * @author lanxia39@163.com
 */
public class StreamComparator implements ComparisonStrategy {

    private static final Logger log = LoggerFactory.getLogger(StreamComparator.class);

    private final JdbcQueryExecutor queryExecutor;

    public StreamComparator() {
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
        log.info("Starting StreamDiff comparison: {} vs {}",
            leftTable.getFullName(), rightTable.getFullName());

        try {
            SqlDialect dialect = resolveDialect(leftDataSource);
            String pkColumn = leftTable.primaryKey().get(0);

            // Key range: use explicit range if provided, otherwise auto-detect
            BigInteger minKey;
            BigInteger maxKey;
            if (options.getMinKey() != null && options.getMaxKey() != null) {
                minKey = toBigInteger(options.getMinKey());
                maxKey = toBigInteger(options.getMaxKey());
            } else {
                minKey = getMinKey(leftDataSource, leftTable, pkColumn, dialect);
                maxKey = getMaxKey(rightDataSource, rightTable, pkColumn, dialect);
            }

            long leftCount = getRowCount(leftDataSource, leftTable, dialect);
            long rightCount = getRowCount(rightDataSource, rightTable, dialect);

            List<DiffRecord> diffs;
            if (options.getThreads() > 1) {
                diffs = compareParallel(leftDataSource, rightDataSource,
                    leftTable, rightTable, options, dialect, minKey, maxKey);
            } else {
                diffs = compareSequential(leftDataSource, rightDataSource,
                    leftTable, rightTable, options, dialect, minKey, maxKey);
            }

            Instant end = Instant.now();
            Duration duration = Duration.between(start, end);

            var stats = new DiffResult.Statistics(
                leftCount, rightCount, 0, 0, 0, 0
            );

            log.info("StreamDiff completed in {}: {} differences found", duration, diffs.size());

            return new DiffResult(diffs, stats, duration);

        } catch (SQLException e) {
            throw new DataDiffException("StreamDiff comparison failed", e);
        }
    }

    /**
     * Sequential chunk-by-chunk comparison.
     */
    private List<DiffRecord> compareSequential(
        DataSource leftDs, DataSource rightDs,
        TableInfo leftTable, TableInfo rightTable,
        CompareOptions options,
        SqlDialect dialect,
        BigInteger minKey, BigInteger maxKey
    ) throws SQLException {
        String pkColumn = leftTable.primaryKey().get(0);
        List<DiffRecord> allDiffs = new ArrayList<>();
        BigInteger currentKey = minKey;
        BigInteger chunkSize = BigInteger.valueOf(options.getBisectionFactor());

        while (currentKey.compareTo(maxKey) <= 0) {
            BigInteger nextKey = currentKey.add(chunkSize);
            BigInteger endKey = nextKey.subtract(BigInteger.ONE);
            if (endKey.compareTo(maxKey) > 0) {
                endKey = maxKey;
            }

            allDiffs.addAll(compareChunk(leftDs, rightDs,
                leftTable, rightTable, pkColumn, dialect, currentKey, endKey));

            currentKey = nextKey;
        }

        return allDiffs;
    }

    /**
     * Parallel chunk comparison.
     */
    private List<DiffRecord> compareParallel(
        DataSource leftDs, DataSource rightDs,
        TableInfo leftTable, TableInfo rightTable,
        CompareOptions options,
        SqlDialect dialect,
        BigInteger minKey, BigInteger maxKey
    ) throws SQLException {
        String pkColumn = leftTable.primaryKey().get(0);
        BigInteger range = maxKey.subtract(minKey).add(BigInteger.ONE);
        long chunkSize = options.getBisectionFactor();
        int numChunks = Math.max(1, range.divide(BigInteger.valueOf(chunkSize)).intValue()
            + (range.mod(BigInteger.valueOf(chunkSize)).signum() > 0 ? 1 : 0));

        ExecutorService executor = Executors.newFixedThreadPool(options.getThreads());
        try {
            List<Future<List<DiffRecord>>> futures = new ArrayList<>();
            BigInteger currentKey = minKey;

            for (int i = 0; i < numChunks; i++) {
                BigInteger startKey = currentKey;
                BigInteger endKey = currentKey.add(BigInteger.valueOf(chunkSize)).subtract(BigInteger.ONE);
                if (endKey.compareTo(maxKey) > 0) endKey = maxKey;

                final BigInteger sk = startKey;
                final BigInteger ek = endKey;
                futures.add(executor.submit(() ->
                    compareChunk(leftDs, rightDs, leftTable, rightTable,
                        pkColumn, dialect, sk, ek)));

                currentKey = endKey.add(BigInteger.ONE);
                if (currentKey.compareTo(maxKey) > 0) break;
            }

            List<DiffRecord> allDiffs = new ArrayList<>();
            for (Future<List<DiffRecord>> future : futures) {
                allDiffs.addAll(future.get());
            }
            return allDiffs;
        } catch (Exception e) {
            throw new DataDiffException("Parallel stream comparison failed", e);
        } finally {
            executor.shutdown();
        }
    }

    private List<DiffRecord> compareChunk(
        DataSource leftDs, DataSource rightDs,
        TableInfo leftTable, TableInfo rightTable,
        String pkColumn, SqlDialect dialect,
        BigInteger startKey, BigInteger endKey
    ) throws SQLException {
        String whereClause = "%s BETWEEN %s AND %s".formatted(
            dialect.quoteIdentifier(pkColumn),
            startKey.toString(),
            endKey.toString()
        );

        String leftSql = "SELECT * FROM %s WHERE %s ORDER BY %s".formatted(
            dialect.quoteIdentifier(leftTable.tableName()),
            whereClause,
            dialect.quoteIdentifier(pkColumn)
        );
        String rightSql = "SELECT * FROM %s WHERE %s ORDER BY %s".formatted(
            dialect.quoteIdentifier(rightTable.tableName()),
            whereClause,
            dialect.quoteIdentifier(pkColumn)
        );

        try (var leftConn = leftDs.getConnection();
             var rightConn = rightDs.getConnection()) {

            List<Map<String, Object>> leftRows = queryExecutor.executeQuery(leftConn, leftSql);
            List<Map<String, Object>> rightRows = queryExecutor.executeQuery(rightConn, rightSql);

            return compareRowSets(leftRows, rightRows, pkColumn);
        }
    }

    private List<DiffRecord> compareRowSets(
        List<Map<String, Object>> leftRows,
        List<Map<String, Object>> rightRows,
        String pkColumn
    ) {
        List<DiffRecord> diffs = new ArrayList<>();

        var leftMap = new LinkedHashMap<>();
        for (Map<String, Object> row : leftRows) {
            leftMap.put(row.get(pkColumn), row);
        }
        var rightMap = new LinkedHashMap<>();
        for (Map<String, Object> row : rightRows) {
            rightMap.put(row.get(pkColumn), row);
        }

        for (var entry : leftMap.entrySet()) {
            Object pk = entry.getKey();
            @SuppressWarnings("unchecked")
            Map<String, Object> leftRow = (Map<String, Object>) entry.getValue();
            @SuppressWarnings("unchecked")
            Map<String, Object> rightRow = (Map<String, Object>) rightMap.get(pk);

            if (rightRow == null) {
                diffs.add(DiffRecord.leftOnly(Map.of(pkColumn, pk), leftRow));
            } else {
                List<String> diffCols = findDifferences(leftRow, rightRow);
                if (!diffCols.isEmpty()) {
                    diffs.add(DiffRecord.modified(
                        Map.of(pkColumn, pk), leftRow, rightRow, diffCols));
                }
            }
        }

        for (var entry : rightMap.entrySet()) {
            Object pk = entry.getKey();
            if (!leftMap.containsKey(pk)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> rightRow = (Map<String, Object>) entry.getValue();
                diffs.add(DiffRecord.rightOnly(Map.of(pkColumn, pk), rightRow));
            }
        }

        return diffs;
    }

    private List<String> findDifferences(Map<String, Object> left, Map<String, Object> right) {
        List<String> diffs = new ArrayList<>();
        for (String col : left.keySet()) {
            if (!java.util.Objects.equals(left.get(col), right.get(col))) {
                diffs.add(col);
            }
        }
        return diffs;
    }

    // ===== Helpers =====

    private SqlDialect resolveDialect(DataSource ds) {
        try (var conn = ds.getConnection()) {
            return DialectResolver.resolve(conn.getMetaData().getURL());
        } catch (SQLException e) {
            throw new DataDiffException("Failed to resolve dialect", e);
        }
    }

    private BigInteger getMinKey(DataSource ds, TableInfo table,
                                  String pkColumn, SqlDialect dialect) throws SQLException {
        String sql = dialect.minMaxQuery(table.tableName(), pkColumn);
        try (var conn = ds.getConnection()) {
            var results = queryExecutor.executeQuery(conn, sql);
            if (!results.isEmpty()) {
                Object val = results.get(0).get("min_val");
                if (val != null) return new BigInteger(val.toString());
            }
        }
        return BigInteger.ZERO;
    }

    private BigInteger getMaxKey(DataSource ds, TableInfo table,
                                  String pkColumn, SqlDialect dialect) throws SQLException {
        String sql = dialect.minMaxQuery(table.tableName(), pkColumn);
        try (var conn = ds.getConnection()) {
            var results = queryExecutor.executeQuery(conn, sql);
            if (!results.isEmpty()) {
                Object val = results.get(0).get("max_val");
                if (val != null) return new BigInteger(val.toString());
            }
        }
        return BigInteger.ZERO;
    }

    private long getRowCount(DataSource ds, TableInfo table,
                              SqlDialect dialect) throws SQLException {
        String sql = dialect.countQuery(table.tableName(), null);
        try (var conn = ds.getConnection()) {
            return queryExecutor.executeScalar(conn, sql, Long.class);
        }
    }

    private BigInteger toBigInteger(Comparable<?> val) {
        if (val instanceof BigInteger bi) return bi;
        if (val instanceof Number num) return BigInteger.valueOf(num.longValue());
        return new BigInteger(val.toString());
    }

    @Override
    public String getStrategyName() {
        return "StreamDiff";
    }
}
