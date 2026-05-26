package com.raksha.booking.lock;

import com.raksha.booking.model.BookingRequest;
import com.raksha.booking.result.BookingResult;
import com.raksha.booking.service.BookingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumer thread that pulls {@link BookingRequest}s from the
 * {@link BookingQueue} and delegates to {@link BookingService}.
 *
 * <p>Each worker runs in a tight loop: dequeue → book → repeat.
 * The loop exits when:
 * <ul>
 *   <li>The thread is interrupted (sets the interrupt flag and returns).</li>
 *   <li>{@link BookingQueue#dequeue()} returns {@code null} (shutdown signal).</li>
 *   <li>{@link #stop()} is called externally (sets {@code running = false}).</li>
 * </ul>
 */
public class BookingWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(BookingWorker.class);

    private final BookingQueue   queue;
    private final BookingService service;
    private final AtomicInteger  successCount;
    private final AtomicInteger  failureCount;

    private volatile boolean running = true;

    public BookingWorker(BookingQueue queue,
                         BookingService service,
                         AtomicInteger successCount,
                         AtomicInteger failureCount) {
        this.queue        = queue;
        this.service      = service;
        this.successCount = successCount;
        this.failureCount = failureCount;
    }

    @Override
    public void run() {
        log.info("Worker {} started", Thread.currentThread().getName());

        while (running) {
            try {
                BookingRequest req = queue.dequeue();
                if (req == null) {
                    // null is the poison-pill from BookingQueue.shutdown()
                    break;
                }

                BookingResult result = service.book(req);

                if (result.isSuccess()) {
                    successCount.incrementAndGet();
                    log.info("[SUCCESS] {} → {}", Thread.currentThread().getName(),
                            result.getMessage());
                } else {
                    failureCount.incrementAndGet();
                    log.info("[FAIL]    {} → {}", Thread.currentThread().getName(),
                            result.getMessage());
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Worker {} interrupted — stopping", Thread.currentThread().getName());
                running = false;
            }
        }

        log.info("Worker {} stopped", Thread.currentThread().getName());
    }

    /** Gracefully signals the worker to stop after the current request. */
    public void stop() {
        running = false;
    }
}
