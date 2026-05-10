package io.sketch.datadiff.output;

import io.sketch.datadiff.core.model.*;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

public class CsvFormatterTest {

    @Test
    void testFormatSingleModifiedRecord() {
        DiffRecord record = DiffRecord.modified(
            Map.of("id", 1),
            Map.of("id", 1, "name", "Alice"),
            Map.of("id", 1, "name", "Bob"),
            List.of("name")
        );
        DiffResult result = new DiffResult(
            List.of(record),
            new DiffResult.Statistics(2, 2, 0, 0, 0, 0),
            Duration.ofSeconds(1)
        );

        CsvFormatter formatter = new CsvFormatter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        formatter.format(result, out);

        String csv = out.toString();
        assertTrue(csv.contains("primary_key,diff_type,left_value,right_value,differing_columns"));
        assertTrue(csv.contains("1,MODIFIED"));
        assertTrue(csv.contains("name"));
    }

    @Test
    void testValuesContainingCommasAreQuoted() {
        DiffRecord record = DiffRecord.modified(
            Map.of("id", 1),
            Map.of("id", 1, "full_name", "Doe, John"),
            Map.of("id", 1, "full_name", "Smith, Jane"),
            List.of("full_name")
        );
        DiffResult result = new DiffResult(List.of(record), DiffResult.Statistics.empty(), Duration.ZERO);

        CsvFormatter formatter = new CsvFormatter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        formatter.format(result, out);

        String csv = out.toString();
        // The map's toString() includes commas, so the entire cell should be quoted.
        // Map.toString() order is undefined; verify quoting is triggered.
        assertTrue(csv.matches("(?s).*\"[^\"]*Doe,[^\"]*John[^\"]*\".*"),
            "CSV should quote values containing commas");
    }

    @Test
    void testValuesContainingQuotesAreEscaped() {
        DiffRecord record = DiffRecord.modified(
            Map.of("id", 1),
            Map.of("id", 1, "note", "He said \"hello\""),
            Map.of("id", 1, "note", "She said \"bye\""),
            List.of("note")
        );
        DiffResult result = new DiffResult(List.of(record), DiffResult.Statistics.empty(), Duration.ZERO);

        CsvFormatter formatter = new CsvFormatter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        formatter.format(result, out);

        String csv = out.toString();
        // Quotes should be doubled for CSV escaping
        assertTrue(csv.contains("\"\"hello\"\""), "CSV should escape quotes by doubling them");
    }

    @Test
    void testAllThreeDiffTypes() {
        List<DiffRecord> records = List.of(
            DiffRecord.leftOnly(Map.of("id", 1),
                Map.of("id", 1, "name", "Alice")),
            DiffRecord.rightOnly(Map.of("id", 2),
                Map.of("id", 2, "name", "Bob")),
            DiffRecord.modified(Map.of("id", 3),
                Map.of("id", 3, "name", "Charlie"),
                Map.of("id", 3, "name", "Charles"),
                List.of("name"))
        );
        DiffResult result = new DiffResult(records, DiffResult.Statistics.empty(), Duration.ZERO);

        CsvFormatter formatter = new CsvFormatter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        formatter.format(result, out);

        String csv = out.toString();
        assertTrue(csv.contains("LEFT_ONLY"));
        assertTrue(csv.contains("RIGHT_ONLY"));
        assertTrue(csv.contains("MODIFIED"));
    }

    @Test
    void testFormatName() {
        assertEquals("csv", new CsvFormatter().getFormatName());
    }
}
