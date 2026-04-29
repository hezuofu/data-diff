package io.sketch.datadiff.parallel;

import io.sketch.datadiff.core.model.DiffRecord;
import io.sketch.datadiff.engine.ChunkProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * ForkJoin-based segment processor for work-stealing parallelism.
 * More efficient for unbalanced workloads.
 */
public class ForkJoinSegmentProcessor implements AutoCloseable {
    
    private final ForkJoinPool forkJoinPool;
    
    public ForkJoinSegmentProcessor(int parallelism) {
        this.forkJoinPool = new ForkJoinPool(parallelism);
    }
    
    public ForkJoinSegmentProcessor() {
        this(Runtime.getRuntime().availableProcessors());
    }
    
    /**
     * Process segments using ForkJoin framework.
     */
    public List<DiffRecord> process(List<ChunkProcessor> processors) {
        if (processors.isEmpty()) {
            return List.of();
        }
        
        SegmentMergeTask task = new SegmentMergeTask(processors, 0, processors.size());
        return forkJoinPool.invoke(task);
    }
    
    /**
     * Recursive task for processing segments.
     */
    private static class SegmentMergeTask extends RecursiveTask<List<DiffRecord>> {
        private final List<ChunkProcessor> processors;
        private final int start;
        private final int end;
        
        SegmentMergeTask(List<ChunkProcessor> processors, int start, int end) {
            this.processors = processors;
            this.start = start;
            this.end = end;
        }
        
        @Override
        protected List<DiffRecord> compute() {
            if (end - start <= 1) {
                // Base case: process single segment
                if (start < end) {
                    try {
                        return processors.get(start).call();
                    } catch (Exception e) {
                        throw new RuntimeException("Segment processing failed", e);
                    }
                }
                return List.of();
            }
            
            // Split task
            int mid = (start + end) / 2;
            SegmentMergeTask leftTask = new SegmentMergeTask(processors, start, mid);
            SegmentMergeTask rightTask = new SegmentMergeTask(processors, mid, end);
            
            leftTask.fork();
            List<DiffRecord> rightResult = rightTask.compute();
            List<DiffRecord> leftResult = leftTask.join();
            
            // Merge results
            List<DiffRecord> result = new ArrayList<>(leftResult);
            result.addAll(rightResult);
            return result;
        }
    }
    
    @Override
    public void close() {
        forkJoinPool.shutdown();
    }
}
