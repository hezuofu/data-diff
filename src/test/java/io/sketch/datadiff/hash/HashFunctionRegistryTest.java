package io.sketch.datadiff.hash;

import io.sketch.datadiff.core.spi.ChecksumCalculator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class HashFunctionRegistryTest {

    @Test
    void testGetBuiltIn() {
        assertTrue(HashFunctionRegistry.get("md5") instanceof Md5Checksum);
        assertTrue(HashFunctionRegistry.get("crc32") instanceof Crc32Checksum);
        assertTrue(HashFunctionRegistry.get("composite") instanceof CompositeChecksum);
    }

    @Test
    void testGetDefault() {
        ChecksumCalculator calc = HashFunctionRegistry.getDefault();
        assertEquals("MD5", calc.getAlgorithmName());
    }

    @Test
    void testGetUnknown() {
        assertThrows(IllegalArgumentException.class, () -> HashFunctionRegistry.get("sha256"));
    }

    @Test
    void testIsRegistered() {
        assertTrue(HashFunctionRegistry.isRegistered("md5"));
        assertTrue(HashFunctionRegistry.isRegistered("CRC32")); // case insensitive
        assertFalse(HashFunctionRegistry.isRegistered("unknown"));
    }

    @Test
    void testGetRegisteredNames() {
        var names = HashFunctionRegistry.getRegisteredNames();
        assertTrue(names.contains("md5"));
        assertTrue(names.contains("crc32"));
        assertTrue(names.contains("composite"));
    }

    @Test
    void testRegisterNewFunction() {
        Crc32Checksum custom = new Crc32Checksum();
        HashFunctionRegistry.register("custom-crc", custom);
        assertEquals(custom, HashFunctionRegistry.get("custom-crc"));
    }
}
