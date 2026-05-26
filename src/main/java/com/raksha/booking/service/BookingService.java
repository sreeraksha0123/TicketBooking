package com.raksha.booking.service;

import com.raksha.booking.config.DatabaseConfig;
import com.raksha.booking.lock.SeatLockManager;
import com.raksha.booking.model.BookingRequest;
import com.raksha.booking.model.Seat;
import com.raksha.booking.repository.SeatRepository;
import com.raksha.booking.result.BookingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Orchestrates the complete booking flow with dual-layer locking.
 *
 * <h2>Flow</h2>
 * <ol>
 *   <li><b>Resolve seat ID</b> — lightweight lookup, no lock held.</li>
 *   <li><b>Acquire ReentrantLock</b> (2 s timeout) — serialises threads at the
 *       JVM level before they touch the DB, reducing wasted round-trips.</li>
 *   <li><b>Open transaction</b> with {@code READ_COMMITTED} isolation.</li>
 *   <li><b>SELECT FOR UPDATE</b> — acquires a PostgreSQL row-level write lock,
 *       the correctness guarantee for multi-instance deployments.</li>
 *   <li><b>Availability check</b> — rejects already-booked seats immediately.</li>
 *   <li><b>UPDATE + INSERT</b> atomically (optimistic version check inside
 *       {@code markBooked}).</li>
 *   <li><b>Commit</b> — releases DB lock.</li>
 *   <li><b>Unlock</b> (always in {@code finally}) — releases JVM lock.</li>
 * </ol>
 *
 * <h2>Why Both Locks?</h2>
 * <ul>
 *   <li>{@link SeatLockManager} works <em>within one JVM</em> — fast, avoids
 *       unnecessary DB round-trips for clear same-process conflicts.</li>
 *   <li>{@code SELECT FOR UPDATE} is the correctness guarantee that survives
 *       multi-instance deployments, JVM crashes, and application-level bugs.
 *       Defence in depth.</li>
 * </ul>
 */
public class BookingService {

    private static final Logger log = LoggerFactory.getLogger(BookingService.class);

    private static final long LOCK_TIMEOUT_MS = 2_000;

    private final SeatLockManager lockMgr;
    private final SeatRepository  repo;

    public BookingService() {
        this(new SeatLockManager(), new SeatRepository());
    }

    /** Constructor for injection (testing). */
    public BookingService(SeatLockManager lockMgr, SeatRepository repo) {
        this.lockMgr = lockMgr;
        this.repo    = repo;
    }

    /**
     * Attempts to book the seat described in {@code req}.
     *
     * @return a {@link BookingResult} — never throws for expected failures
     *         (seat taken, lock timeout); unexpected DB errors return failure
     *         results with the error message.
     */
    public BookingResult book(BookingRequest req) {
        int seatId;

        // ── Step 1: resolve seat ID (no lock needed) ──────────────────────────
        try (Connection conn = DatabaseConfig.getConnection()) {
            seatId = repo.findSeatId(req.eventId(), req.seatNumber(), conn);
        } catch (SQLException e) {
            return BookingResult.failure("DB lookup error: " + e.getMessage());
        } catch (RuntimeException e) {
            return BookingResult.failure(e.getMessage()); // seat not found
        }

        // ── Step 2: acquire in-process lock ───────────────────────────────────
        boolean acquired = lockMgr.tryLock(seatId, LOCK_TIMEOUT_MS);
        if (!acquired) {
            return BookingResult.failure(
                    "Seat lock timeout for " + req.seatNumber() + " — try again");
        }

        // ── Steps 3-7: DB transaction (lock released in finally) ──────────────
        try (Connection conn = DatabaseConfig.getConnection()) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            try {
                // Step 3: SELECT FOR UPDATE — acquires DB row lock
                Seat seat = repo.findAndLock(req.eventId(), req.seatNumber(), conn);

                // Step 4: availability check
                if (!seat.isAvailable()) {
                    conn.rollback();
                    return BookingResult.failure("Seat " + req.seatNumber() + " already booked");
                }

                // Steps 5-6: UPDATE (optimistic version check) + INSERT booking
                repo.markBooked(seat.getId(), seat.getVersion(), conn);
                repo.insertBooking(seat.getId(), req.userId(), conn);

                // Step 7: commit — releases DB row lock
                conn.commit();

                log.info("Booking committed: user={} seat={}", req.userId(), req.seatNumber());
                return BookingResult.success(seat.getSeatNumber());

            } catch (Exception e) {
                try { conn.rollback(); } catch (SQLException ex) {
                    log.error("Rollback failed", ex);
                }
                log.warn("Booking failed for user={} seat={}: {}",
                        req.userId(), req.seatNumber(), e.getMessage());
                return BookingResult.failure(e.getMessage());
            }

        } catch (SQLException e) {
            return BookingResult.failure("DB error: " + e.getMessage());

        } finally {
            // ALWAYS release the JVM lock — forgetting this causes deadlock
            lockMgr.unlock(seatId);
        }
    }

    /** Exposes lock manager for observability (e.g. timeout count). */
    public SeatLockManager getLockManager() { return lockMgr; }
}
