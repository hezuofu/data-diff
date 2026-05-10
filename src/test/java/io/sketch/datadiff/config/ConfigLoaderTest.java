package io.sketch.datadiff.config;

import org.junit.jupiter.api.Test;
import java.io.File;
import static org.junit.jupiter.api.Assertions.*;

public class ConfigLoaderTest {

    @Test
    void testLoadValidConfig() {
        AppConfig config = ConfigLoader.load("src/test/resources/test-valid.conf");
        assertNotNull(config);
        assertEquals("jdbc:mysql://localhost:3306/db1", config.left().url());
        assertEquals("hashdiff", config.comparison().algorithm());
    }

    @Test
    void testLoadInvalidConfig() {
        assertThrows(RuntimeException.class, () -> 
            ConfigLoader.load("src/test/resources/test-invalid.conf")
        );
    }

    @Test
    void testFileNotFound() {
        assertThrows(IllegalArgumentException.class, () -> 
            ConfigLoader.load("non-existent.conf")
        );
    }
}
