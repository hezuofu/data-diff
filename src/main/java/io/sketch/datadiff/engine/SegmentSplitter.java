package io.sketch.datadiff.engine;

import io.sketch.datadiff.core.model.Segment;
import io.sketch.datadiff.core.strategy.SplitStrategy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Segment splitter that divides data ranges into manageable chunks.
 *
 * @author lanxia39@163.com
 *
 * @author lanxia39@163.com
 */
public class SegmentSplitter implements SplitStrategy {
    
    @Override
    public List<Segment> split(BigInteger minKey, BigInteger maxKey, long totalCount, long segmentSize) {
        if (minKey.compareTo(maxKey) > 0) {
            throw new IllegalArgumentException("minKey must be <= maxKey");
        }
        if (segmentSize <= 0) {
            throw new IllegalArgumentException("segmentSize must be > 0");
        }
        
        List<Segment> segments = new ArrayList<>();
        BigInteger range = maxKey.subtract(minKey).add(BigInteger.ONE);
        long numSegments = Math.max(1, totalCount / segmentSize);
        BigInteger segmentRange = range.divide(BigInteger.valueOf(numSegments));
        
        if (segmentRange.equals(BigInteger.ZERO)) {
            segmentRange = BigInteger.ONE;
        }
        
        BigInteger current = minKey;
        long remainingCount = totalCount;
        
        while (current.compareTo(maxKey) <= 0) {
            BigInteger next = current.add(segmentRange).subtract(BigInteger.ONE);
            if (next.compareTo(maxKey) > 0) {
                next = maxKey;
            }
            
            long segmentCount = remainingCount / (numSegments - segments.size());
            segments.add(new Segment(current, next, segmentCount));
            
            current = next.add(BigInteger.ONE);
        }
        
        return segments;
    }
}
