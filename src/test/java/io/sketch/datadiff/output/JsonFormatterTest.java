package io.sketch.datadiff.output;

import io.sketch.datadiff.core.model.*;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

public class JsonFormatterTest {

    @Test
    void testFormat() {
        DiffRecord record = DiffRecord.modified(
            Map.of("id", 1),
            Map.of("id", 1, "name", "Alice"),
            Map.of("id", 1, "name", "Bob"),
            List.of("name")
        );
        
        DiffResult result = new DiffResult(
            List.of(record),
            new DiffResult.Statistics(3, 3, 1, 1, 0, 0),
            Duration.ofSeconds(1)
        );
        
        JsonFormatter formatter = new JsonFormatter();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        formatter.format(result, out);
        
        String json = out.toString();
        assertTrue(json.contains("\"diffCount\" : 1"));
        assertTrue(json.contains("\"hasDifferences\" : true"));
        assertTrue(json.contains("\"MODIFIED\" : 1"));
    }

    @Test
    void testFormatName() {
        assertEquals("json", new JsonFormatter().getFormatName());
    }
}
