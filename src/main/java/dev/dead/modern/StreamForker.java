package dev.dead.modern;

import dev.dead.common.Key;
import dev.dead.common.Results;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Runs several stream functions concurrently over ONE traversal of the source.
 * The caller's thread pushes elements, in batches, into one bounded queue per fork;
 * each fork pulls them back out as a Stream on its own virtual thread.
 */
public final class StreamForker<T> {

    private static final int BATCH_SIZE = 512;   // elements per queue hand-off
    private static final int QUEUE_BATCHES = 8;  // bounded number of batches in flight per fork

    private final Stream<T> source;
    private final Map<Key<?>, Function<Stream<T>, ?>> forks = new LinkedHashMap<>();

    private StreamForker(Stream<T> source) {
        this.source = Objects.requireNonNull(source);
    }

    public static <T> StreamForker<T> from(Stream<T> source) {
        return new StreamForker<>(source);
    }

    private static <T> void broadcast(List<BlockingQueue<List<T>>> queues, List<T> batch) {
        try {
            for (var queue : queues) {
                queue.put(batch);                      // blocks while a fork is behind: back-pressure
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CancellationException("Interrupted while feeding the forks");
        }
    }

    private static <T, R> R runFork(Function<Stream<T>, R> operation, BlockingQueue<List<T>> queue,
                                    List<T> end, AtomicBoolean failed) {
        var elements = new QueueSpliterator<>(queue, end);
        try {
            return operation.apply(StreamSupport.stream(elements, false));
        } catch (Throwable t) {
            failed.set(true);
            throw t;
        } finally {
            elements.drain();                          // a finished fork cannot block the producer
        }
    }

    // ---- producer side: pull from the source, push to every queue --------------------------------

    private static Results join(Map<Key<?>, Future<?>> futures) {
        var values = new LinkedHashMap<Key<?>, Object>();
        CompletionException failure = null;
        for (var entry : futures.entrySet()) {
            try {
                values.put(entry.getKey(), entry.getValue().get());
            } catch (ExecutionException e) {
                var wrapped = new CompletionException("Fork '" + entry.getKey() + "' failed", e.getCause());
                if (failure == null) {
                    failure = wrapped;
                } else {
                    failure.addSuppressed(wrapped);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CancellationException("Interrupted while waiting for the forks");
            }
        }
        if (failure != null) {
            throw failure;
        }
        return new Results(values);
    }

    public <R> StreamForker<T> fork(Key<R> key, Function<Stream<T>, R> operation) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(operation, "operation");
        if (forks.putIfAbsent(key, operation) != null) {
            throw new IllegalArgumentException("Duplicate key '" + key + "'");
        }
        return this;
    }

    /**
     * Traverses the source once and returns when every fork has finished.
     */
    public Results run() {
        final var end = new ArrayList<T>();            // poison pill, recognised by identity
        final var failed = new AtomicBoolean();        // a failed fork tells the producer to stop
        final var queues = new ArrayList<BlockingQueue<List<T>>>();
        final var futures = new LinkedHashMap<Key<?>, Future<?>>();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); source) {
            forks.forEach((key, operation) -> {
                var queue = new ArrayBlockingQueue<List<T>>(QUEUE_BATCHES);
                queues.add(queue);
                futures.put(key, executor.submit(() -> runFork(operation, queue, end, failed)));
            });
            try {
                feed(queues, end, failed);
            } catch (Throwable t) {
                executor.shutdownNow();                // end may not have reached every fork
                throw t;
            }
            return join(futures);
        }
    }

    // ---- consumer side: one virtual thread per fork ----------------------------------------------

    private void feed(List<BlockingQueue<List<T>>> queues, List<T> end, AtomicBoolean failed) {
        var batcher = new Batcher<T>(queues);
        var elements = source.sequential().spliterator();
        while (!failed.get() && elements.tryAdvance(batcher)) {
            // one element per call
        }
        batcher.flush();
        broadcast(queues, end);
    }

    private static final class Batcher<T> implements Consumer<T> {
        private final List<BlockingQueue<List<T>>> queues;
        private List<T> batch = new ArrayList<>(BATCH_SIZE);

        Batcher(List<BlockingQueue<List<T>>> queues) {
            this.queues = queues;
        }

        @Override
        public void accept(T element) {
            batch.add(element);
            if (batch.size() == BATCH_SIZE) {
                flush();
            }
        }

        void flush() {
            if (batch.isEmpty()) {
                return;
            }
            broadcast(queues, batch);                  // the same read-only batch is shared by all forks
            batch = new ArrayList<>(BATCH_SIZE);
        }
    }

    /**
     * Push-to-pull adapter: elements arrive, batch by batch, through a BlockingQueue.
     */
    private static final class QueueSpliterator<T> extends Spliterators.AbstractSpliterator<T> {
        private final BlockingQueue<List<T>> queue;
        private final List<T> end;
        private Iterator<T> current = Collections.emptyIterator();
        private boolean finished;

        QueueSpliterator(BlockingQueue<List<T>> queue, List<T> end) {
            super(Long.MAX_VALUE, ORDERED);            // size unknown; FIFO arrival preserves encounter order
            this.queue = queue;
            this.end = end;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            while (!current.hasNext()) {
                if (finished) {
                    return false;
                }
                var batch = take();
                if (batch == end) {
                    finished = true;
                    return false;
                }
                current = batch.iterator();
            }
            action.accept(current.next());
            return true;
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;                               // a live queue cannot be split
        }

        void drain() {
            try {
                while (!finished) {
                    if (take() == end) {
                        finished = true;
                    }
                }
            } catch (CancellationException _) {
                // interrupted: stop draining (the interrupt flag is already restored)
            }
        }

        private List<T> take() {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CancellationException("Interrupted while waiting for elements");
            }
        }
    }
}
