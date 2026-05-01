package io.sketch.datadiff.engine;

import io.sketch.datadiff.core.model.Segment;
import io.sketch.datadiff.core.strategy.SplitStrategy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Segment splitter that divides data ranges into manageable chunks.
 * Uses bisection factor (number of initial segments) rather than fixed segment size.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class SegmentSplitter implements SplitStrategy {
    
    @Override
    public List<Segment> split(BigInteger minKey, BigInteger maxKey, long totalCount, long bisectionFactor) {
        if (minKey.compareTo(maxKey) > 0) {
            throw new IllegalArgumentException("minKey must be <= maxKey");
        }
        if (bisectionFactor <= 0) {
            throw new IllegalArgumentException("bisectionFactor must be > 0");
        }
        
        List<Segment> segments = new ArrayList<>();
        BigInteger range = maxKey.subtract(minKey).add(BigInteger.ONE);
        
        long numSegments = Math.min(bisectionFactor, Math.max(1, totalCount));
        if (numSegments == 0) numSegments = 1;
        
        BigInteger segmentRange = range.divide(BigInteger.valueOf(numSegments));
        if (segmentRange.equals(BigInteger.ZERO)) {
            segmentRange = BigInteger.ONE;
        }
        
        BigInteger current = minKey;
        long remainingCount = totalCount;
        
        while (current.compareTo(maxKey) <= 0) {
            BigInteger next;
            if (segments.size() == numSegments - 1) {
                next = maxKey;
            } else {
                next = current.add(segmentRange).subtract(BigInteger.ONE);
                if (next.compareTo(maxKey) > 0) {
                    next = maxKey;
                }
            }
            
            long divisor = numSegments - segments.size();
            long segmentCount = divisor > 0 ? remainingCount / divisor : remainingCount;
            segments.add(new Segment(current, next, segmentCount));
            
            remainingCount -= segmentCount;
            current = next.add(BigInteger.ONE);
        }
        
        return segments;
    }
}
