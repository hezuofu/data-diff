package io.sketch.datadiff.stream;

import io.sketch.datadiff.core.model.DiffRecord;

import java.util.*;
import java.util.function.Function;

/**
 * Stream merger for combining multiple sorted diff streams.
 * Uses k-way merge algorithm for efficient merging.
 */
public class StreamMerger {
    
    /**
     * Merge multiple sorted lists of diff records.
     * 
     * @param streams list of sorted diff record lists
     * @param keyExtractor function to extract sort key from diff record
     * @return merged sorted list
     */
    public static List<DiffRecord> merge(
        List<List<DiffRecord>> streams,
        Function<DiffRecord, Comparable<?>> keyExtractor
    ) {
        if (streams == null || streams.isEmpty()) {
            return List.of();
        }
        
        if (streams.size() == 1) {
            return streams.get(0);
        }
        
        // Use priority queue for k-way merge
        PriorityQueue<StreamItem> pq = new PriorityQueue<>();
        
        // Initialize with first item from each stream
        List<Iterator<DiffRecord>> iterators = new ArrayList<>();
        for (List<DiffRecord> stream : streams) {
            iterators.add(stream.iterator());
        }
        
        for (int i = 0; i < iterators.size(); i++) {
            Iterator<DiffRecord> it = iterators.get(i);
            if (it.hasNext()) {
                DiffRecord record = it.next();
                Comparable<?> key = keyExtractor.apply(record);
                pq.add(new StreamItem(key, record, i));
            }
        }
        
        // Merge
        List<DiffRecord> result = new ArrayList<>();
        while (!pq.isEmpty()) {
            StreamItem item = pq.poll();
            result.add(item.record);
            
            // Add next item from the same stream
            Iterator<DiffRecord> it = iterators.get(item.streamIndex);
            if (it.hasNext()) {
                DiffRecord next = it.next();
                Comparable<?> key = keyExtractor.apply(next);
                pq.add(new StreamItem(key, next, item.streamIndex));
            }
        }
        
        return result;
    }
    
    /**
     * Merge multiple streams using natural ordering (by primary key).
     */
    public static List<DiffRecord> merge(List<List<DiffRecord>> streams) {
        return merge(streams, record -> {
            Object pk = record.primaryKey().values().iterator().next();
            return (Comparable<?>) pk;
        });
    }
    
    /**
     * Merge two sorted lists.
     */
    public static List<DiffRecord> merge(
        List<DiffRecord> left,
        List<DiffRecord> right,
        Function<DiffRecord, Comparable<?>> keyExtractor
    ) {
        return merge(List.of(left, right), keyExtractor);
    }
    
    /**
     * Stream item for priority queue.
     */
    private static class StreamItem implements Comparable<StreamItem> {
        private final Comparable<?> key;
        private final DiffRecord record;
        private final int streamIndex;
        
        StreamItem(Comparable<?> key, DiffRecord record, int streamIndex) {
            this.key = key;
            this.record = record;
            this.streamIndex = streamIndex;
        }
        
        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public int compareTo(StreamItem other) {
            return ((Comparable) this.key).compareTo(other.key);
        }
    }
    
    /**
     * Concatenate multiple streams without sorting.
     */
    public static List<DiffRecord> concatenate(List<List<DiffRecord>> streams) {
        if (streams == null || streams.isEmpty()) {
            return List.of();
        }
        
        return streams.stream()
            .flatMap(List::stream)
            .toList();
    }
}
