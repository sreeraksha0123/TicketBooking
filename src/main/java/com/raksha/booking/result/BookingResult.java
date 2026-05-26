package com.raksha.booking.result;

/**
 * Result of a booking attempt.
 *
 * <p>Follows the Result / Either pattern — avoids throwing exceptions for
 * expected outcomes (seat already booked, lock timeout) while still
 * distinguishing them from unexpected failures.
 */
public final class BookingResult {

    private final boolean success;
    private final String  message;

    private BookingResult(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public static BookingResult success(String seatNumber) {
        return new BookingResult(true, "Booked seat " + seatNumber);
    }

    public static BookingResult failure(String reason) {
        return new BookingResult(false, reason);
    }

    public boolean isSuccess()  { return success; }
    public String  getMessage() { return message; }

    @Override
    public String toString() {
        return (success ? "[OK] " : "[FAIL] ") + message;
    }
}
