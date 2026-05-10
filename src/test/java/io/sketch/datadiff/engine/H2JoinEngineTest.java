package io.sketch.datadiff.engine;

import io.sketch.datadiff.builder.DataDiffBuilder;
import io.sketch.datadiff.core.model.*;
import io.sketch.datadiff.datasource.pool.HikariCPProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@Disabled("H2 does not support FULL OUTER JOIN natively")
public class H2JoinEngineTest {

    private static final String URL = "jdbc:h2:mem:jointest;DB_CLOSE_DELAY=-1";

    @BeforeEach
    void setup() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL, "sa", "");
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS USERS_LEFT");
            stmt.execute("DROP TABLE IF EXISTS USERS_RIGHT");
            stmt.execute("CREATE TABLE USERS_LEFT (ID INT PRIMARY KEY, NAME VARCHAR(50), AGE INT)");
            stmt.execute("CREATE TABLE USERS_RIGHT (ID INT PRIMARY KEY, NAME VARCHAR(50), AGE INT)");
            
            stmt.execute("INSERT INTO USERS_LEFT VALUES (1, 'Alice', 25)");
            stmt.execute("INSERT INTO USERS_LEFT VALUES (2, 'Bob', 30)");
            stmt.execute("INSERT INTO USERS_LEFT VALUES (3, 'Charlie', 35)");
            
            stmt.execute("INSERT INTO USERS_RIGHT VALUES (1, 'Alice', 25)"); // Same
            stmt.execute("INSERT INTO USERS_RIGHT VALUES (2, 'Robert', 30)"); // Updated
            stmt.execute("INSERT INTO USERS_RIGHT VALUES (4, 'David', 40)"); // New
        }
    }

    @Test
    void testJoinDiff() {
        Properties props = new Properties();
        props.setProperty("user", "sa");
        props.setProperty("password", "");
        
        DataSource ds = HikariCPProvider.createDataSource(URL, props);

        CompareOptions options = CompareOptions.builder()
            .strategy(CompareOptions.StrategyType.JOIN)
            .build();

        List<ColumnDef> columns = List.of(
            new ColumnDef("ID", "INTEGER", false),
            new ColumnDef("NAME", "VARCHAR", true),
            new ColumnDef("AGE", "INTEGER", true)
        );

        TableInfo leftTable = new TableInfo("USERS_LEFT", columns, List.of("ID"));
        TableInfo rightTable = new TableInfo("USERS_RIGHT", columns, List.of("ID"));

        DiffResult result = new DataDiffBuilder()
            .bothDataSource(ds)
            .options(options)
            .build()
            .compare(leftTable, rightTable, options);

        assertNotNull(result);
        List<DiffRecord> diffs = result.getDiffRecords();
        
        // Expected differences:
        // ID 2: MODIFIED
        // ID 3: LEFT_ONLY
        // ID 4: RIGHT_ONLY
        
        assertEquals(3, diffs.size());
        
        assertTrue(diffs.stream().anyMatch(d -> d.diffType() == DiffType.MODIFIED && d.primaryKey().get("ID").equals(2)));
        assertTrue(diffs.stream().anyMatch(d -> d.diffType() == DiffType.LEFT_ONLY && d.primaryKey().get("ID").equals(3)));
        assertTrue(diffs.stream().anyMatch(d -> d.diffType() == DiffType.RIGHT_ONLY && d.primaryKey().get("ID").equals(4)));
    }
}
