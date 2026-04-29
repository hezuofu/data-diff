package io.sketch.datadiff.parallel;

import io.sketch.datadiff.core.model.DiffRecord;
import io.sketch.datadiff.engine.ChunkProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Parallel segment processor using thread pool.
 * Processes multiple segments concurrently.
 *
 * @author lanxia39@163.com
 *
 * 
 */
public class ParallelSegmentProcessor implements AutoCloseable {
    
    private final ExecutorService executor;
    private final int parallelism;
    
    public ParallelSegmentProcessor(int parallelism) {
        this.parallelism = parallelism;
        this.executor = Executors.newFixedThreadPool(parallelism);
    }
    
    /**
     * Process multiple segments in parallel.
     */
    public List<DiffRecord> processAll(List<ChunkProcessor> processors) throws Exception {
        List<Future<List<DiffRecord>>> futures = new ArrayList<>();
        
        for (ChunkProcessor processor : processors) {
            futures.add(executor.submit(processor));
        }
        
        List<DiffRecord> results = new ArrayList<>();
        for (Future<List<DiffRecord>> future : futures) {
            results.addAll(future.get());
        }
        
        return results;
    }
    
    /**
     * Get parallelism level.
     */
    public int getParallelism() {
        return parallelism;
    }
    
    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
