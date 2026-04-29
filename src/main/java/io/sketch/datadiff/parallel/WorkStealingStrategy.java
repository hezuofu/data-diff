package io.sketch.datadiff.parallel;

import io.sketch.datadiff.core.model.DiffRecord;
import io.sketch.datadiff.engine.ChunkProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Work-stealing strategy for dynamic load balancing.
 * Idle workers steal tasks from busy workers' queues.
 */
public class WorkStealingStrategy implements AutoCloseable {
    
    private final ForkJoinPool workStealingPool;
    private final Queue<ChunkProcessor> taskQueue;
    private final AtomicInteger processedCount;
    private final AtomicInteger failedCount;
    
    public WorkStealingStrategy(int parallelism) {
        this.workStealingPool = new ForkJoinPool(parallelism);
        this.taskQueue = new ConcurrentLinkedQueue<>();
        this.processedCount = new AtomicInteger(0);
        this.failedCount = new AtomicInteger(0);
    }
    
    /**
     * Submit tasks for processing.
     */
    public void submitTasks(List<ChunkProcessor> processors) {
        taskQueue.addAll(processors);
    }
    
    /**
     * Execute all submitted tasks with work-stealing.
     */
    public List<DiffRecord> executeAll() {
        List<DiffRecord> allResults = new ArrayList<>();
        List<Callable<List<DiffRecord>>> tasks = new ArrayList<>();
        
        while (!taskQueue.isEmpty()) {
            ChunkProcessor processor = taskQueue.poll();
            if (processor != null) {
                tasks.add(processor);
            }
        }
        
        try {
            List<Future<List<DiffRecord>>> futures = workStealingPool.invokeAll(tasks);
            
            for (Future<List<DiffRecord>> future : futures) {
                try {
                    List<DiffRecord> result = future.get();
                    allResults.addAll(result);
                    processedCount.incrementAndGet();
                } catch (ExecutionException e) {
                    failedCount.incrementAndGet();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            workStealingPool.shutdown();
        }
        
        return allResults;
    }
    
    /**
     * Get number of processed tasks.
     */
    public int getProcessedCount() {
        return processedCount.get();
    }
    
    /**
     * Get number of failed tasks.
     */
    public int getFailedCount() {
        return failedCount.get();
    }
    
    /**
     * Get remaining tasks count.
     */
    public int getRemainingTasks() {
        return taskQueue.size();
    }
    
    /**
     * Check if all tasks are completed.
     */
    public boolean isCompleted() {
        return taskQueue.isEmpty() && processedCount.get() + failedCount.get() > 0;
    }
    
    /**
     * Reset counters.
     */
    public void reset() {
        processedCount.set(0);
        failedCount.set(0);
        taskQueue.clear();
    }
    
    @Override
    public void close() {
        workStealingPool.shutdown();
        try {
            if (!workStealingPool.awaitTermination(60, TimeUnit.SECONDS)) {
                workStealingPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            workStealingPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
