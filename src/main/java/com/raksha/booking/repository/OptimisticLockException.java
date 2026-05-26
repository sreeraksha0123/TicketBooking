package com.raksha.booking.repository;

/**
 * Thrown when an optimistic-lock conflict is detected.
 *
 * <p>This happens when {@code markBooked} executes
 * {@code WHERE id = ? AND version = ?} but finds 0 rows updated,
 * indicating that another transaction already incremented the version
 * between our SELECT and UPDATE.
 */
public class OptimisticLockException extends RuntimeException {

    public OptimisticLockException(String message) {
        super(message);
    }
}
