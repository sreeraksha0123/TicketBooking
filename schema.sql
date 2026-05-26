-- ============================================================
--  Concurrent Ticket Booking System — Database Schema
--  PostgreSQL 14+
-- ============================================================

-- Drop in reverse dependency order (safe re-run)
DROP TABLE IF EXISTS bookings;
DROP TABLE IF EXISTS seats;
DROP TABLE IF EXISTS events;

-- ── Events ────────────────────────────────────────────────
CREATE TABLE events (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    venue       VARCHAR(200) NOT NULL,
    event_date  TIMESTAMP    NOT NULL
);

-- ── Seats ─────────────────────────────────────────────────
--  status  : AVAILABLE | BOOKED
--  version : optimistic-lock counter — incremented on every UPDATE
CREATE TABLE seats (
    id          SERIAL PRIMARY KEY,
    event_id    INT          NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    seat_number VARCHAR(10)  NOT NULL,
    status      VARCHAR(20)  NOT NULL DEFAULT 'AVAILABLE'
                             CHECK (status IN ('AVAILABLE', 'BOOKED')),
    version     INT          NOT NULL DEFAULT 0,
    UNIQUE (event_id, seat_number)
);

-- Index for fast seat lookup during booking
CREATE INDEX idx_seats_event_status ON seats(event_id, status);

-- ── Bookings ───────────────────────────────────────────────
--  UNIQUE(seat_id) is the last-resort DB guard:
--  even if application-level locking fails, the DB will
--  reject the second INSERT with a unique-constraint violation.
CREATE TABLE bookings (
    id        SERIAL    PRIMARY KEY,
    seat_id   INT       NOT NULL REFERENCES seats(id),
    user_id   INT       NOT NULL,
    booked_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (seat_id)   -- one booking per seat — ever
);

-- ── Seed Data ─────────────────────────────────────────────
-- 1 event
INSERT INTO events (name, venue, event_date)
VALUES ('Coldplay World Tour', 'Bengaluru Arena', '2025-12-15 19:00:00');

-- 100 seats: A1 … A100
INSERT INTO seats (event_id, seat_number, status)
SELECT 1, 'A' || generate_series(1, 100), 'AVAILABLE';
