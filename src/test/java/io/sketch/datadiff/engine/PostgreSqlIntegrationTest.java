package io.sketch.datadiff.engine;

import io.sketch.datadiff.builder.DataDiffBuilder;
import io.sketch.datadiff.core.model.*;
import io.sketch.datadiff.datasource.pool.HikariCPProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@Disabled("Requires Docker environment")
public class PostgreSqlIntegrationTest {

    @Container
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @BeforeAll
    static void setup() throws Exception {
        try (Connection conn = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement stmt = conn.createStatement()) {
            
            // Create table in public schema (default)
            stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
            
            // Insert some data
            stmt.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
            stmt.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
            stmt.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");
            
            // Create another table to compare with
            stmt.execute("CREATE TABLE users_diff (id INT PRIMARY KEY, name VARCHAR(50), age INT)");
            stmt.execute("INSERT INTO users_diff VALUES (1, 'Alice', 25)"); // Same
            stmt.execute("INSERT INTO users_diff VALUES (2, 'Robert', 30)"); // Updated name
            stmt.execute("INSERT INTO users_diff VALUES (4, 'David', 40)"); // New record
        }
    }

    @Test
    void testHashDiff() {
        Properties props = new Properties();
        props.setProperty("user", postgres.getUsername());
        props.setProperty("password", postgres.getPassword());
        
        DataSource leftDs = HikariCPProvider.createDataSource(postgres.getJdbcUrl(), props);
        DataSource rightDs = HikariCPProvider.createDataSource(postgres.getJdbcUrl(), props);

        CompareOptions options = CompareOptions.builder()
            .strategy(CompareOptions.StrategyType.HASH)
            .build();

        List<ColumnDef> columns = List.of(
            new ColumnDef("id", "INTEGER", false),
            new ColumnDef("name", "VARCHAR", true),
            new ColumnDef("age", "INTEGER", true)
        );

        TableInfo leftTable = new TableInfo("users", columns, List.of("id"));
        TableInfo rightTable = new TableInfo("users_diff", columns, List.of("id"));

        DiffResult result = new DataDiffBuilder()
            .leftDataSource(leftDs)
            .rightDataSource(rightDs)
            .options(options)
            .build()
            .compare(leftTable, rightTable, options);

        assertNotNull(result);
        List<DiffRecord> diffs = result.getDiffRecords();
        
        // Diffs expected:
        // ID 2: MODIFIED
        // ID 3: LEFT_ONLY
        // ID 4: RIGHT_ONLY
        
        assertEquals(3, diffs.size());
        
        assertTrue(diffs.stream().anyMatch(d -> d.diffType() == DiffType.MODIFIED && d.primaryKey().get("id").equals(2)));
        assertTrue(diffs.stream().anyMatch(d -> d.diffType() == DiffType.LEFT_ONLY && d.primaryKey().get("id").equals(3)));
        assertTrue(diffs.stream().anyMatch(d -> d.diffType() == DiffType.RIGHT_ONLY && d.primaryKey().get("id").equals(4)));
    }
}
