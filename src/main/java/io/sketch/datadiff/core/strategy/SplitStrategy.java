package io.sketch.datadiff.core.strategy;

import io.sketch.datadiff.core.model.Segment;
import java.math.BigInteger;
import java.util.List;

/**
 * Strategy interface for splitting data into segments.
 */
@FunctionalInterface
public interface SplitStrategy {
    
    /**
     * Split a data range into segments.
     * 
     * @param minKey minimum key value
     * @param maxKey maximum key value
     * @param totalCount total number of rows
     * @param segmentSize target size for each segment
     * @return list of segments covering the range
     */
    List<Segment> split(BigInteger minKey, BigInteger maxKey, long totalCount, long segmentSize);
}
