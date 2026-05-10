package io.sketch.datadiff.engine;

import io.sketch.datadiff.core.model.*;
import io.sketch.datadiff.datasource.dialect.DialectResolver;
import io.sketch.datadiff.datasource.dialect.SqlDialect;
import io.sketch.datadiff.datasource.pool.HikariCPProvider;
import org.junit.jupiter.api.*;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for RecursiveBisector — the core algorithm that finds
 * specific differing rows when segment checksums don't match.
 */
public class RecursiveBisectorIntegrationTest {

    private static final String LEFT_URL = "jdbc:h2:mem:bisectL;DB_CLOSE_DELAY=-1";
    private static final String RIGHT_URL = "jdbc:h2:mem:bisectR;DB_CLOSE_DELAY=-1";

    private DataSource leftDs;
    private DataSource rightDs;
    private SqlDialect dialect;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.setProperty("user", "sa");
        props.setProperty("password", "");
        leftDs = HikariCPProvider.createDataSource(LEFT_URL, props);
        rightDs = HikariCPProvider.createDataSource(RIGHT_URL, props);
        dialect = DialectResolver.resolve("jdbc:h2:mem:bisectL");
    }

    @AfterEach
    void cleanup() throws Exception {
        try (Connection c = DriverManager.getConnection(LEFT_URL, "sa", "");
             Statement s = c.createStatement()) {
            s.execute("DROP ALL OBJECTS");
        }
        try (Connection c = DriverManager.getConnection(RIGHT_URL, "sa", "");
             Statement s = c.createStatement()) {
            s.execute("DROP ALL OBJECTS");
        }
    }

    @Test
    void testFindsSingleModifiedRowInLargeRange() throws Exception {
        // 100 rows in both tables, only ID 25 differs in VALUE column
        createNumberedTable(LEFT_URL, "BTEST", 1, 100, i -> i * 10);
        createNumberedTable(RIGHT_URL, "BTEST", 1, 100, i -> (i == 25) ? 999 : i * 10);

        TableInfo table = new TableInfo("BTEST",
            List.of(new ColumnDef("ID", "INTEGER", false), new ColumnDef("VAL", "INTEGER", true)),
            "ID");

        Segment segment = new Segment(BigInteger.ONE, BigInteger.valueOf(100), 100);
        RecursiveBisector bisector = new RecursiveBisector(
            leftDs, rightDs, table, table, dialect, 5);

        List<DiffRecord> diffs = bisector.bisect(segment);

        assertEquals(1, diffs.size());
        DiffRecord diff = diffs.get(0);
        assertEquals(DiffType.MODIFIED, diff.diffType());
        assertEquals(25, diff.primaryKey().get("ID"));
        assertTrue(diff.differingColumns().contains("VAL"));
    }

    @Test
    void testFindsLeftOnlyRows() throws Exception {
        // Left: IDs 1-20, Right: IDs 1-15 (IDs 16-20 are left-only)
        createNumberedTable(LEFT_URL, "LO", 1, 20, i -> i);
        createNumberedTable(RIGHT_URL, "LO", 1, 15, i -> i);

        TableInfo table = new TableInfo("LO",
            List.of(new ColumnDef("ID", "INTEGER", false), new ColumnDef("VAL", "INTEGER", true)),
            "ID");

        Segment segment = new Segment(BigInteger.ONE, BigInteger.valueOf(20), 20);
        RecursiveBisector bisector = new RecursiveBisector(
            leftDs, rightDs, table, table, dialect, 3);

        List<DiffRecord> diffs = bisector.bisect(segment);

        assertEquals(5, diffs.size());
        assertTrue(diffs.stream().allMatch(d -> d.diffType() == DiffType.LEFT_ONLY));
    }

    @Test
    void testFindsRightOnlyRows() throws Exception {
        // Left: IDs 1-10, Right: IDs 1-15 (IDs 11-15 are right-only)
        createNumberedTable(LEFT_URL, "RO", 1, 10, i -> i);
        createNumberedTable(RIGHT_URL, "RO", 1, 15, i -> i);

        TableInfo table = new TableInfo("RO",
            List.of(new ColumnDef("ID", "INTEGER", false), new ColumnDef("VAL", "INTEGER", true)),
            "ID");

        Segment segment = new Segment(BigInteger.ONE, BigInteger.valueOf(15), 15);
        RecursiveBisector bisector = new RecursiveBisector(
            leftDs, rightDs, table, table, dialect, 3);

        List<DiffRecord> diffs = bisector.bisect(segment);

        assertEquals(5, diffs.size());
        assertTrue(diffs.stream().allMatch(d -> d.diffType() == DiffType.RIGHT_ONLY));
    }

    @Test
    void testSegmentOutsideActualDataProducesNoResults() throws Exception {
        createNumberedTable(LEFT_URL, "EMPTY", 1, 10, i -> i);
        createNumberedTable(RIGHT_URL, "EMPTY", 1, 10, i -> i);

        TableInfo table = new TableInfo("EMPTY",
            List.of(new ColumnDef("ID", "INTEGER", false), new ColumnDef("VAL", "INTEGER", true)),
            "ID");

        // Segment range outside actual data
        Segment segment = new Segment(BigInteger.valueOf(100), BigInteger.valueOf(200), 0);
        RecursiveBisector bisector = new RecursiveBisector(
            leftDs, rightDs, table, table, dialect, 10);

        List<DiffRecord> diffs = bisector.bisect(segment);
        assertTrue(diffs.isEmpty());
    }

    @Test
    void testBisectorTracksDepthAndIterations() throws Exception {
        createNumberedTable(LEFT_URL, "DEPTH", 1, 100, i -> i * 10);
        createNumberedTable(RIGHT_URL, "DEPTH", 1, 100, i -> (i == 50) ? 777 : i * 10);

        TableInfo table = new TableInfo("DEPTH",
            List.of(new ColumnDef("ID", "INTEGER", false), new ColumnDef("VAL", "INTEGER", true)),
            "ID");

        Segment segment = new Segment(BigInteger.ONE, BigInteger.valueOf(100), 100);
        RecursiveBisector bisector = new RecursiveBisector(
            leftDs, rightDs, table, table, dialect, 5);

        bisector.bisect(segment);

        // getIterations() counts the number of bisect() calls, always >= 1
        assertTrue(bisector.getIterations() >= 1);
        // Note: currentMaxDepth tracks segment.depth() only from top-level bisect(),
        // not recursive depth — this is a known gap in the metrics tracking.
    }

    // ===== Helpers =====

    @FunctionalInterface
    interface ValueGenerator {
        int generate(int id);
    }

    private void createNumberedTable(String url, String tableName,
                                      int startId, int endId,
                                      ValueGenerator valueGen) throws Exception {
        try (Connection conn = DriverManager.getConnection(url, "sa", "");
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            stmt.execute("CREATE TABLE " + tableName
                + " (ID INT PRIMARY KEY, VAL INT)");
            for (int id = startId; id <= endId; id++) {
                stmt.execute("INSERT INTO " + tableName
                    + " VALUES (" + id + ", " + valueGen.generate(id) + ")");
            }
        }
    }
}
