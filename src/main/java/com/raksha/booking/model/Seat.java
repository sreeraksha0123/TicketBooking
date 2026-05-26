package com.raksha.booking.model;

/**
 * Domain model for a seat in an event.
 *
 * <p>{@code status} is declared {@code volatile} so that reads from any thread
 * always see the latest write without requiring explicit synchronisation on
 * the field itself. The authoritative state is always in the database;
 * this in-memory value is used for quick availability checks only.
 *
 * <p>{@code version} is the optimistic-lock counter: every UPDATE increments it,
 * and the {@code WHERE id = ? AND version = ?} clause in
 * {@link com.raksha.booking.repository.SeatRepository#markBooked} will update
 * 0 rows if another transaction already incremented the version — surfaced as
 * an {@link com.raksha.booking.repository.OptimisticLockException}.
 */
public class Seat {

    private final int    id;
    private final int    eventId;
    private final String seatNumber;
    private volatile String status;   // AVAILABLE | BOOKED
    private int          version;

    public Seat(int id, int eventId, String seatNumber, String status, int version) {
        this.id         = id;
        this.eventId    = eventId;
        this.seatNumber = seatNumber;
        this.status     = status;
        this.version    = version;
    }

    // ── Convenience ──────────────────────────────────────

    public boolean isAvailable() {
        return "AVAILABLE".equals(status);
    }

    // ── Getters / Setters ─────────────────────────────────

    public int getId()           { return id; }
    public int getEventId()      { return eventId; }
    public String getSeatNumber(){ return seatNumber; }
    public String getStatus()    { return status; }
    public int getVersion()      { return version; }

    public void setStatus(String status)   { this.status  = status; }
    public void setVersion(int version)    { this.version = version; }

    @Override
    public String toString() {
        return "Seat{id=" + id + ", seat=" + seatNumber
                + ", status=" + status + ", v=" + version + "}";
    }
}
