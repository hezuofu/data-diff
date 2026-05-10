package io.sketch.datadiff.engine;

import io.sketch.datadiff.core.model.Segment;
import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class SegmentSplitterTest {

    private final SegmentSplitter splitter = new SegmentSplitter();

    @Test
    void testSplitWithGaps() {
        // ID range 1-1000, 100 rows, bisectionFactor=4 → 4 segments
        List<Segment> segments = splitter.split(
            BigInteger.ONE, 
            BigInteger.valueOf(1000), 
            100, 
            4
        );
        
        assertEquals(4, segments.size());
        assertEquals(BigInteger.ONE, segments.get(0).rangeStart());
        assertEquals(BigInteger.valueOf(1000), segments.get(3).rangeEnd());
        
        // Check row distribution (approx 25 rows per segment)
        assertEquals(25, segments.get(0).count());
        assertEquals(25, segments.get(1).count());
    }

    @Test
    void testHugeIdRange() {
        // ID range is massive (e.g. Snowflake IDs), bisectionFactor=2 → 2 segments
        BigInteger start = new BigInteger("178485747384950272");
        BigInteger end = start.add(BigInteger.valueOf(1000000));
        
        List<Segment> segments = splitter.split(start, end, 10, 2);
        assertEquals(2, segments.size());
        assertEquals(start, segments.get(0).rangeStart());
        assertEquals(end, segments.get(1).rangeEnd());
    }

    @Test
    void testSingleRow() {
        List<Segment> segments = splitter.split(BigInteger.TEN, BigInteger.TEN, 1, 100);
        assertEquals(1, segments.size());
        assertEquals(BigInteger.TEN, segments.get(0).rangeStart());
        assertEquals(BigInteger.TEN, segments.get(0).rangeEnd());
        assertEquals(1, segments.get(0).count());
    }

    @Test
    void testSingleSegment() {
        List<Segment> segments = splitter.split(BigInteger.ONE, BigInteger.TEN, 10, 1);
        assertEquals(1, segments.size());
        assertEquals(BigInteger.ONE, segments.get(0).rangeStart());
        assertEquals(BigInteger.TEN, segments.get(0).rangeEnd());
    }
}
