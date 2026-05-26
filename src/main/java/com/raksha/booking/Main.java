package com.raksha.booking;

import com.raksha.booking.config.DatabaseConfig;
import com.raksha.booking.lock.BookingQueue;
import com.raksha.booking.lock.BookingWorker;
import com.raksha.booking.model.BookingRequest;
import com.raksha.booking.service.BookingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simulation Runner — entry point.
 *
 * <h2>Scenario</h2>
 * <ul>
 *   <li>200 concurrent users all attempt to book seats {@code A1–A100}.</li>
 *   <li>10 worker threads process requests from a shared {@link BookingQueue}.</li>
 *   <li>Expected outcome: exactly <b>100 successes</b>, 100 failures — zero
 *       double-bookings.</li>
 * </ul>
 *
 * <h2>Run</h2>
 * <pre>
 *   # Start PostgreSQL
 *   docker compose up -d
 *
 *   # Build
 *   mvn package -q
 *
 *   # Run simulation
 *   java -jar target/ticket-booking.jar
 * </pre>
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    // ── Configuration ─────────────────────────────────────────────────────────
    private static final int WORKER_COUNT   = 10;
    private static final int USER_COUNT     = 200;
    private static final int SEAT_COUNT     = 100;
    private static final int EVENT_ID       = 1;
    private static final int QUEUE_CAPACITY = 100;
    private static final int DRAIN_WAIT_MS  = 5_000;

    public static void main(String[] args) throws Exception {

        printBanner();

        // ── Setup ─────────────────────────────────────────────────────────────
        BookingQueue   queue   = new BookingQueue(QUEUE_CAPACITY);
        BookingService service = new BookingService();

        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger failures  = new AtomicInteger(0);

        // ── Launch worker threads (consumers) ─────────────────────────────────
        List<BookingWorker> workers = new ArrayList<>();
        List<Thread>        workerThreads = new ArrayList<>();

        for (int i = 0; i < WORKER_COUNT; i++) {
            BookingWorker worker = new BookingWorker(queue, service, successes, failures);
            Thread t = new Thread(worker, "worker-" + i);
            t.setDaemon(true);
            t.start();
            workers.add(worker);
            workerThreads.add(t);
        }

        log.info("Started {} worker threads", WORKER_COUNT);

        // ── Simulate 200 concurrent users (producers) ─────────────────────────
        CountDownLatch enqueueLatch = new CountDownLatch(USER_COUNT);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < USER_COUNT; i++) {
            final int userId = i + 1;
            // Each user tries to book their modular seat so every seat gets 2 attempts
            String seat = "A" + (i % SEAT_COUNT + 1);

            new Thread(() -> {
                try {
                    queue.enqueue(new BookingRequest(userId, EVENT_ID, seat));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    enqueueLatch.countDown();
                }
            }, "user-" + userId).start();
        }

        // Wait for all users to have enqueued their requests
        enqueueLatch.await();
        log.info("All {} requests enqueued. Waiting for workers to drain…", USER_COUNT);

        // Give workers time to drain the queue
        Thread.sleep(DRAIN_WAIT_MS);

        // ── Graceful shutdown ─────────────────────────────────────────────────
        queue.shutdown();
        for (Thread t : workerThreads) {
            t.join(2_000);
        }

        long elapsed = System.currentTimeMillis() - startTime;

        // ── Print Results ─────────────────────────────────────────────────────
        printResults(successes.get(), failures.get(), elapsed,
                service.getLockManager().getTimeoutCount());

        // ── DB Verification ───────────────────────────────────────────────────
        verifyDatabase();

        DatabaseConfig.shutdown();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void printBanner() {
        System.out.println("""
                ╔══════════════════════════════════════════════════════╗
                ║       Concurrent Ticket Booking — Simulation         ║
                ║  200 users · 100 seats · 10 workers · 1 event        ║
                ╚══════════════════════════════════════════════════════╝
                """);
    }

    private static void printResults(int successes, int failures,
                                     long elapsedMs, int lockTimeouts) {
        System.out.printf("""
                %n┌────────────────────────────────────────┐
                │           Simulation Results           │
                ├────────────────────────────────────────┤
                │  Successful bookings : %4d (expect 100)│
                │  Failed bookings     : %4d (expect 100)│
                │  Lock timeouts       : %4d             │
                │  Wall-clock time     : %4d ms          │
                └────────────────────────────────────────┘%n""",
                successes, failures, lockTimeouts, elapsedMs);
    }

    private static void verifyDatabase() {
        System.out.println("── DB Verification ─────────────────────────────────");
        try (Connection conn = DatabaseConfig.getConnection();
             Statement  stmt = conn.createStatement()) {

            ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) AS total FROM bookings WHERE seat_id IN " +
                    "(SELECT id FROM seats WHERE event_id = " + EVENT_ID + ")");
            if (rs.next()) {
                int dbCount = rs.getInt("total");
                String verdict = (dbCount == SEAT_COUNT) ? "✓ PASS" : "✗ FAIL";
                System.out.printf("  bookings in DB : %d  (expected %d)  %s%n",
                        dbCount, SEAT_COUNT, verdict);
            }

            // Check for double-bookings (should always be 0)
            rs = stmt.executeQuery(
                    "SELECT COUNT(*) AS dupes FROM (" +
                    "  SELECT seat_id, COUNT(*) c FROM bookings GROUP BY seat_id" +
                    "  HAVING COUNT(*) > 1) t");
            if (rs.next()) {
                int dupes = rs.getInt("dupes");
                System.out.printf("  double-bookings : %d  (expected 0)  %s%n",
                        dupes, dupes == 0 ? "✓ PASS" : "✗ FAIL ← BUG!");
            }

        } catch (SQLException e) {
            log.error("DB verification failed", e);
        }
        System.out.println("────────────────────────────────────────────────────");
    }
}
