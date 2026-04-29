package io.sketch.datadiff.core.model;

import java.math.BigInteger;

/**
 * Segment record representing a range of data to be compared.
 * 
 * @param rangeStart start of the primary key range (inclusive)
 * @param rangeEnd end of the primary key range (inclusive)
 * @param count estimated row count in this segment
 * @param checksum pre-computed checksum (optional, may be null)
 * @param depth recursion depth for bisection
 *
 * @author lanxia39@163.com
 */
public record Segment(
    BigInteger rangeStart,
    BigInteger rangeEnd,
    long count,
    BigInteger checksum,
    int depth
) {
    public Segment {
        if (rangeStart == null || rangeEnd == null) {
            throw new IllegalArgumentException("Range boundaries cannot be null");
        }
        if (rangeStart.compareTo(rangeEnd) > 0) {
            throw new IllegalArgumentException("rangeStart must be <= rangeEnd");
        }
        if (count < 0) {
            throw new IllegalArgumentException("Count cannot be negative");
        }
        if (depth < 0) {
            throw new IllegalArgumentException("Depth cannot be negative");
        }
    }
    
    /**
     * Create a segment without checksum.
     */
    public Segment(BigInteger rangeStart, BigInteger rangeEnd, long count, int depth) {
        this(rangeStart, rangeEnd, count, null, depth);
    }
    
    /**
     * Create a segment at depth 0.
     */
    public Segment(BigInteger rangeStart, BigInteger rangeEnd, long count) {
        this(rangeStart, rangeEnd, count, null, 0);
    }
    
    /**
     * Create a copy of this segment with a new checksum.
     */
    public Segment withChecksum(BigInteger checksum) {
        return new Segment(rangeStart, rangeEnd, count, checksum, depth);
    }
    
    /**
     * Create a copy of this segment with a new count.
     */
    public Segment withCount(long count) {
        return new Segment(rangeStart, rangeEnd, count, checksum, depth);
    }
    
    /**
     * Create a copy of this segment with incremented depth.
     */
    public Segment withDeeperDepth() {
        return new Segment(rangeStart, rangeEnd, count, checksum, depth + 1);
    }
    
    /**
     * Get the size of this segment's range.
     */
    public BigInteger rangeSize() {
        return rangeEnd.subtract(rangeStart);
    }
    
    /**
     * Check if this segment has a computed checksum.
     */
    public boolean hasChecksum() {
        return checksum != null;
    }
    
    /**
     * Split this segment into two equal halves.
     * 
     * @return array of two segments [left, right]
     */
    public Segment[] bisect() {
        BigInteger mid = rangeStart.add(rangeEnd).divide(BigInteger.valueOf(2));
        Segment left = new Segment(rangeStart, mid, count / 2, checksum, depth + 1);
        Segment right = new Segment(mid.add(BigInteger.ONE), rangeEnd, count - (count / 2), checksum, depth + 1);
        return new Segment[]{left, right};
    }
    
    @Override
    public String toString() {
        return "Segment[%s-%s, depth=%d]".formatted(
            rangeStart, rangeEnd, depth
        );
    }
}
