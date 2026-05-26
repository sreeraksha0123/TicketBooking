package com.raksha.booking.lock;

import com.raksha.booking.model.BookingRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * Bounded blocking queue for {@link BookingRequest} objects.
 *
 * <h2>Producer-Consumer Pattern</h2>
 * <ul>
 *   <li><b>Producers</b> — request handler threads calling {@link #enqueue}.
 *       If the queue is full they {@code wait()}, releasing the monitor and
 *       parking until a consumer drains a slot.</li>
 *   <li><b>Consumers</b> — {@link BookingWorker} threads calling {@link #dequeue}.
 *       If the queue is empty they {@code wait()}, parking until a producer
 *       adds an item.</li>
 * </ul>
 *
 * <h2>Spurious Wakeup Guard</h2>
 * Both {@code enqueue} and {@code dequeue} use {@code while} loops (not {@code if})
 * to re-check the condition after every {@code wait()} return. The JVM specification
 * permits {@code wait()} to return without a matching {@code notify()} — known as a
 * spurious wakeup. Using {@code if} would proceed assuming the condition is met,
 * introducing a data race under load.
 *
 * <h2>Thread Safety</h2>
 * The entire object acts as the monitor; all public mutating methods are
 * {@code synchronized}.
 */
public class BookingQueue {

    private static final Logger log = LoggerFactory.getLogger(BookingQueue.class);

    private final LinkedList<BookingRequest> queue = new LinkedList<>();
    private final int capacity;
    private volatile boolean shuttingDown = false;

    public BookingQueue(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        this.capacity = capacity;
    }

    // ── Producer ──────────────────────────────────────────────────────────────

    /**
     * Enqueues a request. Blocks if the queue is at capacity.
     *
     * @throws InterruptedException  if the thread is interrupted while waiting
     * @throws IllegalStateException if the queue is shutting down
     */
    public synchronized void enqueue(BookingRequest req) throws InterruptedException {
        if (shuttingDown) throw new IllegalStateException("Queue is shutting down");

        // WHILE — spurious-wakeup guard
        while (queue.size() == capacity) {
            log.debug("Queue full ({}/{}), producer waiting", queue.size(), capacity);
            wait();
        }

        queue.addLast(req);
        log.debug("Enqueued: {} (queueSize={})", req, queue.size());
        notifyAll(); // wake any waiting consumers
    }

    // ── Consumer ──────────────────────────────────────────────────────────────

    /**
     * Dequeues and returns the next request. Blocks if the queue is empty.
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     * @return {@code null} if the queue is shutting down and empty (signal to stop)
     */
    public synchronized BookingRequest dequeue() throws InterruptedException {
        // WHILE — spurious-wakeup guard
        while (queue.isEmpty()) {
            if (shuttingDown) return null; // poison pill
            log.debug("Queue empty, consumer waiting");
            wait();
        }

        BookingRequest req = queue.removeFirst();
        notifyAll(); // wake any waiting producers
        return req;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /**
     * Signals all waiting consumers to drain the queue and stop.
     * Subsequent {@link #enqueue} calls will throw.
     */
    public synchronized void shutdown() {
        shuttingDown = true;
        notifyAll();
    }

    // ── Observability ─────────────────────────────────────────────────────────

    public synchronized int size()     { return queue.size(); }
    public int              capacity() { return capacity; }
    public boolean          isShuttingDown() { return shuttingDown; }
}
