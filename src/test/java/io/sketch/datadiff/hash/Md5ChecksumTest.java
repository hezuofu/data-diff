package io.sketch.datadiff.hash;

import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import static org.junit.jupiter.api.Assertions.*;

public class Md5ChecksumTest {

    private final Md5Checksum calculator = new Md5Checksum();

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
    void testVariousTypes() {
        // MD5 should produce different hashes for different types even if string representation is same
        BigInteger hashInt = calculator.checksum(123);
        BigInteger hashStr = calculator.checksum("123");
        assertNotEquals(hashInt, hashStr, "Hash for Integer and String should differ");
    }

    @Test
    void testLongString() {
        String longStr = "a".repeat(10000);
        assertNotNull(calculator.checksum(longStr));
    }

    @Test
    void testSpecialCharacters() {
        BigInteger hash1 = calculator.checksum("你好世界");
        BigInteger hash2 = calculator.checksum("你好世界");
        assertEquals(hash1, hash2);
        
        BigInteger hash3 = calculator.checksum("hello\nworld\t");
        assertNotEquals(hash1, hash3);
    }
}
