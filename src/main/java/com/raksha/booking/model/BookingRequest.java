package com.raksha.booking.model;

/**
 * Immutable value object representing a single seat-booking request.
 *
 * <p>Uses a Java 16+ {@code record} — automatically generates
 * constructor, accessors, {@code equals}, {@code hashCode}, and {@code toString}.
 *
 * @param userId     ID of the user placing the booking
 * @param eventId    ID of the event
 * @param seatNumber seat label, e.g. {@code "A42"}
 */
public record BookingRequest(int userId, int eventId, String seatNumber) {

    // Compact constructor — validation
    public BookingRequest {
        if (seatNumber == null || seatNumber.isBlank()) {
            throw new IllegalArgumentException("seatNumber must not be blank");
        }
        if (userId <= 0 || eventId <= 0) {
            throw new IllegalArgumentException("userId and eventId must be positive");
        }
    }
}
