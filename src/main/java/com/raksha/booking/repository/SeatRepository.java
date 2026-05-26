package com.raksha.booking.repository;

import com.raksha.booking.model.Seat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Data-access layer for {@link Seat} and booking records.
 *
 * <h2>Locking Strategy</h2>
 * <ul>
 *   <li>{@link #findAndLock} issues {@code SELECT … FOR UPDATE} which acquires
 *       a PostgreSQL row-level lock. Concurrent transactions that try to lock
 *       the same row will block until this transaction commits or rolls back.</li>
 *   <li>{@link #markBooked} uses an optimistic version check
 *       ({@code WHERE id = ? AND version = ?}) as a second safety net. A 0-row
 *       update throws {@link OptimisticLockException}.</li>
 *   <li>{@code UNIQUE(seat_id)} on the {@code bookings} table is the ultimate
 *       DB-level guard against double-booking.</li>
 * </ul>
 *
 * <p>All methods accept an explicit {@link Connection} — callers manage the
 * transaction boundary (begin / commit / rollback).
 */
public class SeatRepository {

    private static final Logger log = LoggerFactory.getLogger(SeatRepository.class);

    // ── Queries ───────────────────────────────────────────────────────────────

    private static final String SQL_FIND_SEAT_ID = """
            SELECT id FROM seats
            WHERE event_id = ? AND seat_number = ?
            """;

    private static final String SQL_FIND_AND_LOCK = """
            SELECT id, event_id, seat_number, status, version
            FROM seats
            WHERE event_id = ? AND seat_number = ?
            FOR UPDATE
            """;

    private static final String SQL_MARK_BOOKED = """
            UPDATE seats
               SET status  = 'BOOKED',
                   version = version + 1
             WHERE id      = ?
               AND version = ?
            """;

    private static final String SQL_INSERT_BOOKING = """
            INSERT INTO bookings (seat_id, user_id)
            VALUES (?, ?)
            """;

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Lightweight lookup — resolves seat label → seat ID without acquiring any lock.
     * Safe to call outside a transaction.
     *
     * @throws RuntimeException if the seat does not exist
     */
    public int findSeatId(int eventId, String seatNumber, Connection conn)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_FIND_SEAT_ID)) {
            ps.setInt(1, eventId);
            ps.setString(2, seatNumber);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                throw new RuntimeException(
                        "Seat not found: event=" + eventId + " seat=" + seatNumber);
            }
            return rs.getInt("id");
        }
    }

    /**
     * Fetches the seat row and acquires a PostgreSQL row-level write lock
     * ({@code FOR UPDATE}). Other transactions trying to lock the same row
     * will block until this transaction commits or rolls back.
     *
     * <p>Must be called within an open, non-auto-commit transaction.
     *
     * @throws RuntimeException if the seat does not exist
     */
    public Seat findAndLock(int eventId, String seatNumber, Connection conn)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_FIND_AND_LOCK)) {
            ps.setInt(1, eventId);
            ps.setString(2, seatNumber);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                throw new RuntimeException(
                        "Seat not found: event=" + eventId + " seat=" + seatNumber);
            }
            Seat seat = mapRow(rs);
            log.debug("Row lock acquired for seat {}", seatNumber);
            return seat;
        }
    }

    /**
     * Atomically marks a seat as BOOKED using an optimistic version check.
     *
     * @throws OptimisticLockException if the version no longer matches
     *         (another transaction already booked this seat)
     */
    public void markBooked(int seatId, int version, Connection conn)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_MARK_BOOKED)) {
            ps.setInt(1, seatId);
            ps.setInt(2, version);
            int rowsUpdated = ps.executeUpdate();
            if (rowsUpdated == 0) {
                throw new OptimisticLockException(
                        "Version conflict on seatId=" + seatId
                        + " (expected version=" + version + ")");
            }
            log.debug("Seat {} marked BOOKED", seatId);
        }
    }

    /**
     * Inserts a booking record.
     *
     * <p>The {@code UNIQUE(seat_id)} constraint on the bookings table will
     * cause this to throw a {@link java.sql.SQLIntegrityConstraintViolationException}
     * if a booking for this seat already exists — last-resort duplicate protection.
     */
    public void insertBooking(int seatId, int userId, Connection conn)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT_BOOKING)) {
            ps.setInt(1, seatId);
            ps.setInt(2, userId);
            ps.executeUpdate();
            log.debug("Booking inserted: seatId={} userId={}", seatId, userId);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Seat mapRow(ResultSet rs) throws SQLException {
        return new Seat(
                rs.getInt("id"),
                rs.getInt("event_id"),
                rs.getString("seat_number"),
                rs.getString("status"),
                rs.getInt("version")
        );
    }
}
