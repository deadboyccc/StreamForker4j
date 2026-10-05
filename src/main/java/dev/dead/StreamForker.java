package dev.dead;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Performs several operations on ONE traversal of a stream (Modern Java in Action, Appendix C).
 * <p>
 * Idea: push every element of the source into one queue per operation; each queue is turned
 * back into its own Stream (via a Spliterator) and processed by its function on its own thread.
 * <p>
 * source.forEach ──► ForkingStreamConsumer ──► queue_i ──► BlockingQueueSpliterator_i ──► Stream_i ──► f_i ──► Future_i
 * <p>
 * Note: the class skeleton (fields, fork, getResults, Results) is not in the excerpt; it is
 * reconstructed from how the later listings use it.
 */
public class StreamForker<T> {

    private final Stream<T> stream;                                          // the single source
    private final Map<Object, Function<Stream<T>, ?>> forks = new HashMap<>(); // key -> operation

    public StreamForker(Stream<T> stream) {
        this.stream = stream;
    }

    /**
     * Registers an operation under a key. Nothing runs until getResults().
     */
    public StreamForker<T> fork(Object key, Function<Stream<T>, ?> f) {
        forks.put(key, f);
        return this;                                                         // fluent API
    }

    /**
     * Starts all operations, traverses the source once (in the CALLER's thread, synchronously)
     * and returns a handle to the results. It does not wait for the operations to finish.
     */
    public Results getResults() {
        ForkingStreamConsumer<T> consumer = build();
        try {
            stream.sequential().forEach(consumer);   // push each element to every queue
        } finally {
            consumer.finish();                       // always signal end, even if the source throws
        }
        return consumer;                             // exposed only through the narrow Results interface
    }

    /**
     * Creates one queue + one running Future per fork and bundles them in a consumer.
     */
    private ForkingStreamConsumer<T> build() {
        List<BlockingQueue<T>> queues = new ArrayList<>();                   // one queue per operation
        Map<Object, Future<?>> actions =                                     // key -> future holding the result
                forks.entrySet().stream().reduce(
                        new HashMap<Object, Future<?>>(),
                        (map, e) -> {
                            map.put(e.getKey(), getOperationResult(queues, e.getValue()));
                            return map;
                        },
                        (m1, m2) -> {
                            m1.putAll(m2);
                            return m1;
                        });                  // combiner (unused: stream is sequential)
        return new ForkingStreamConsumer<>(queues, actions);
    }

    // ---- Listing C.2 --------------------------------------------------------------------------

    /**
     * Wires one fork: queue -> Spliterator -> Stream -> async function.
     */
    private Future<?> getOperationResult(List<BlockingQueue<T>> queues, Function<Stream<T>, ?> f) {
        BlockingQueue<T> queue = new LinkedBlockingQueue<>();                // unbounded mailbox for this fork
        queues.add(queue);                                                   // the consumer will feed it
        Spliterator<T> spliterator = new BlockingQueueSpliterator<>(queue);  // pulls elements from the queue
        Stream<T> source = StreamSupport.stream(spliterator, false);         // sequential Stream over the queue
        // Runs now, on a pool thread; it blocks on the (still empty) queue until elements are pushed.
        return CompletableFuture.supplyAsync(() -> f.apply(source));
    }

    // ---- Listing C.3 --------------------------------------------------------------------------

    /**
     * Read side of the API: blocks until the operation registered under key has completed.
     */
    public interface Results {
        <R> R get(Object key);
    }

    // ---- Listing C.4 --------------------------------------------------------------------------

    /**
     * Push side: broadcasts every element to all queues. Also doubles as the Results handle.
     */
    static class ForkingStreamConsumer<T> implements Consumer<T>, Results {
        static final Object END_OF_STREAM = new Object();   // poison pill, compared by identity

        private final List<BlockingQueue<T>> queues;
        private final Map<Object, Future<?>> actions;

        ForkingStreamConsumer(List<BlockingQueue<T>> queues, Map<Object, Future<?>> actions) {
            this.queues = queues;
            this.actions = actions;
        }

        /**
         * Called by source.forEach: copy the element into every queue.
         */
        @Override
        public void accept(T t) {
            queues.forEach(q -> q.add(t));
        }

        /**
         * Sends the poison pill through the same path as a normal element.
         */
        @SuppressWarnings("unchecked")
        void finish() {
            accept((T) END_OF_STREAM);                // unchecked cast is safe: erased at runtime
        }

        /**
         * Blocks until the operation's Future completes, then returns its result.
         */
        @Override
        @SuppressWarnings("unchecked")
        public <R> R get(Object key) {
            try {
                return ((Future<R>) actions.get(key)).get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    // ---- Listing C.5 --------------------------------------------------------------------------

    /**
     * Pull side: a Spliterator whose elements come from a BlockingQueue. It exists only to
     * turn "elements pushed into a queue" into "a Stream that pulls elements".
     */
    static class BlockingQueueSpliterator<T> implements Spliterator<T> {
        private final BlockingQueue<T> q;

        BlockingQueueSpliterator(BlockingQueue<T> q) {
            this.q = q;
        }

        /**
         * Waits for the next element; false when the poison pill arrives.
         */
        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            T t;
            while (true) {
                try {
                    t = q.take();                     // blocks until the producer pushes something
                    break;
                } catch (InterruptedException _) {
                }  // swallowed: this fork cannot be cancelled
            }
            if (t != ForkingStreamConsumer.END_OF_STREAM) {
                action.accept(t);                     // hand the element to the stream pipeline
                return true;                          // there may be more
            }
            return false;                             // pill seen: stream is finished
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }  // a live queue cannot be split

        @Override
        public long estimateSize() {
            return 0;
        }     // size unknown (Long.MAX_VALUE is more correct)

        @Override
        public int characteristics() {
            return 0;
        }     // no ORDERED/SIZED/... flags
    }
}