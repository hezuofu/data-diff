package io.sketch.datadiff.core.strategy;

import io.sketch.datadiff.core.model.Segment;
import java.math.BigInteger;
import java.util.List;

/**
 * Strategy interface for splitting data into segments.
 *
 * @author lanxia39@163.com
 *
 * 
 */
@FunctionalInterface
public interface SplitStrategy {
    
    /**
     * Split a data range into segments.
     * 
     * @param minKey minimum key value
     * @param maxKey maximum key value
     * @param totalCount total number of rows
     * @param bisectionFactor number of initial segments to split into
     * @return list of segments covering the range
     */
    List<Segment> split(BigInteger minKey, BigInteger maxKey, long totalCount, long bisectionFactor);
}
