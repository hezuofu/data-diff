package io.sketch.datadiff.core.model;

import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import static org.junit.jupiter.api.Assertions.*;

public class SegmentTest {

    @Test
    void testSegmentValidation() {
        assertThrows(IllegalArgumentException.class, () -> 
            new Segment(BigInteger.TEN, BigInteger.ONE, 100)
        );
        assertThrows(IllegalArgumentException.class, () -> 
            new Segment(BigInteger.ONE, BigInteger.TEN, -1)
        );
    }

    @Test
    void testBisect() {
        Segment segment = new Segment(BigInteger.ONE, BigInteger.TEN, 100);
        Segment[] halves = segment.bisect();
        
        assertEquals(2, halves.length);
        assertEquals(BigInteger.ONE, halves[0].rangeStart());
        assertEquals(BigInteger.valueOf(5), halves[0].rangeEnd());
        assertEquals(50, halves[0].count());
        
        assertEquals(BigInteger.valueOf(6), halves[1].rangeStart());
        assertEquals(BigInteger.TEN, halves[1].rangeEnd());
        assertEquals(50, halves[1].count());
    }

    @Test
    void testRangeSize() {
        Segment segment = new Segment(BigInteger.ONE, BigInteger.TEN, 100);
        assertEquals(BigInteger.valueOf(9), segment.rangeSize());
    }
}
