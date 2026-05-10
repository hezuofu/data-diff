package io.sketch.datadiff.hash;

import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import static org.junit.jupiter.api.Assertions.*;

public class Crc32ChecksumTest {

    private final Crc32Checksum calculator = new Crc32Checksum();

    @Test
    void testChecksumConsistency() {
        String value = "test-data";
        BigInteger hash1 = calculator.checksum(value);
        BigInteger hash2 = calculator.checksum(value);
        assertEquals(hash1, hash2);
    }

    @Test
    void testDifferentValues() {
        BigInteger hash1 = calculator.checksum("value1");
        BigInteger hash2 = calculator.checksum("value2");
        assertNotEquals(hash1, hash2);
    }

    @Test
    void testNullValue() {
        assertEquals(BigInteger.ZERO, calculator.checksum((Object) null));
    }

    @Test
    void testAlgorithmName() {
        assertEquals("CRC32", calculator.getAlgorithmName());
    }
}
