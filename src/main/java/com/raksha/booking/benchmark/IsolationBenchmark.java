package com.raksha.booking.benchmark;

import com.raksha.booking.config.DatabaseConfig;
import com.raksha.booking.model.Seat;
import com.raksha.booking.repository.SeatRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmarks the throughput difference between
 * {@code READ COMMITTED} and {@code SERIALIZABLE} isolation levels.
 *
 * <h2>Methodology</h2>
 * <ul>
 *   <li>50 threads × 20 bookings each = 1 000 total operations per run.</li>
 *   <li>A {@link CountDownLatch} "start gate" fires all threads simultaneously
 *       to maximise contention.</li>
 *   <li>Each thread accumulates its own wall-clock time; the total is the sum
 *       across all threads.</li>
 *   <li>The benchmark is run twice — once per isolation level — and the overhead
 *       percentage is printed.</li>
 * </ul>
 *
 * <h2>Expected Result</h2>
 * SERIALIZABLE is typically ~30–40 % slower due to predicate locking
 * (gap locks that prevent phantom reads). The exact number from your
 * run is the figure you cite on your resume.
 *
 * <h2>Run</h2>
 * <pre>
 *   mvn package -q
 *   java -cp target/ticket-booking.jar com.raksha.booking.benchmark.IsolationBenchmark
 * </pre>
 */
public class IsolationBenchmark {

    private static final Logger log = LoggerFactory.getLogger(IsolationBenchmark.class);

    private static final int THREADS             = 50;
    private static final int BOOKINGS_PER_THREAD = 20;
    private static final int SEATS               = 100;
    private static final int EVENT_ID            = 1;

    public static void main(String[] args) throws Exception {
        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║     Isolation Level Benchmark            ║");
        System.out.printf ("║  %d threads × %d ops = %d total ops    ║%n",
                THREADS, BOOKINGS_PER_THREAD, THREADS * BOOKINGS_PER_THREAD);
        System.out.println("╚══════════════════════════════════════════╝");
        System.out.println();

        System.out.println("=== READ COMMITTED ===");
        long rcTime = benchmark(Connection.TRANSACTION_READ_COMMITTED,
                THREADS, BOOKINGS_PER_THREAD);

        // Brief pause between runs so DB can settle
        Thread.sleep(1_000);

        System.out.println("\n=== SERIALIZABLE ===");
        long srTime = benchmark(Connection.TRANSACTION_SERIALIZABLE,
                THREADS, BOOKINGS_PER_THREAD);

        double overhead = ((double)(srTime - rcTime) / rcTime) * 100.0;
        System.out.printf("%n┌─────────────────────────────────────────┐%n");
        System.out.printf("│  READ COMMITTED  total ms : %,8d     │%n", rcTime);
        System.out.printf("│  SERIALIZABLE    total ms : %,8d     │%n", srTime);
        System.out.printf("│  SERIALIZABLE overhead    :   %+6.1f %%  │%n", overhead);
        System.out.printf("└─────────────────────────────────────────┘%n");
    }

    // ── Core Benchmark ────────────────────────────────────────────────────────

    private static long benchmark(int isolationLevel, int threads, int count)
            throws InterruptedException {

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate   = new CountDownLatch(threads);
        AtomicLong  totalTime  = new AtomicLong(0);
        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger failures  = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            final int userId = i + 1;
            new Thread(() -> {
                try {
                    startGate.await(); // all threads start simultaneously
                    long t0 = System.currentTimeMillis();

                    for (int j = 0; j < count; j++) {
                        try {
                            runBooking(userId, j % SEATS, isolationLevel);
                            successes.incrementAndGet();
                        } catch (Exception e) {
                            failures.incrementAndGet();
                        }
                    }

                    totalTime.addAndGet(System.currentTimeMillis() - t0);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endGate.countDown();
                }
            }).start();
        }

        startGate.countDown(); // fire starting gun
        endGate.await();       // wait for all threads to finish

        System.out.printf("  Successes: %4d | Failures: %4d | Total ms: %,d%n",
                successes.get(), failures.get(), totalTime.get());
        return totalTime.get();
    }

    // ── Single Booking ────────────────────────────────────────────────────────

    private static void runBooking(int userId, int seatIdx, int isolation)
            throws SQLException {
        try (Connection conn = DatabaseConfig.getConnection()) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(isolation);
            try {
                SeatRepository repo = new SeatRepository();
                String seatNumber = "A" + (seatIdx + 1);
                Seat seat = repo.findAndLock(EVENT_ID, seatNumber, conn);

                if (!seat.isAvailable()) {
                    conn.rollback();
                    throw new RuntimeException("Already booked: " + seatNumber);
                }

                repo.markBooked(seat.getId(), seat.getVersion(), conn);
                repo.insertBooking(seat.getId(), userId, conn);
                conn.commit();

            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }
}
