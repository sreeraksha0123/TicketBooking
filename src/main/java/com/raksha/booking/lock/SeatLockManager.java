package com.raksha.booking.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-process seat locking layer using one {@link ReentrantLock} per seat ID.
 *
 * <h2>Why ReentrantLock + SELECT FOR UPDATE (dual-layer)?</h2>
 * <ul>
 *   <li><b>ReentrantLock</b> — fast, in-process gate. Threads contending for
 *       the same seat are serialised <em>before</em> they even touch the DB,
 *       reducing unnecessary DB round-trips and lock contention.</li>
 *   <li><b>SELECT FOR UPDATE</b> — correctness guarantee. Survives:
 *       multi-instance deployments, JVM crashes between lock-acquire and DB write,
 *       and any application-level locking bug.</li>
 * </ul>
 *
 * <h2>Fair Lock</h2>
 * Locks are created with {@code fair=true} so threads acquire in FIFO order,
 * preventing thread starvation under high contention.
 *
 * <h2>Thread Safety</h2>
 * {@link ConcurrentHashMap#computeIfAbsent} guarantees at-most-one lock object
 * per seat ID without requiring external synchronisation.
 */
public class SeatLockManager {

    private static final Logger log = LoggerFactory.getLogger(SeatLockManager.class);

    /** One fair ReentrantLock per seat ID — created lazily, never removed. */
    private final ConcurrentHashMap<Integer, ReentrantLock> lockMap =
            new ConcurrentHashMap<>();

    /** Counts how many tryLock calls timed out (useful for observability). */
    private final AtomicInteger timeoutCount = new AtomicInteger(0);

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Returns the lock for the given seat, creating it if absent.
     * Safe to call from multiple threads simultaneously.
     */
    public ReentrantLock getLock(int seatId) {
        return lockMap.computeIfAbsent(seatId, id -> new ReentrantLock(true));
    }

    /**
     * Tries to acquire the lock for {@code seatId} within {@code timeoutMs}.
     *
     * @return {@code true}  if the lock was acquired
     *         {@code false} if the timeout elapsed or the thread was interrupted
     */
    public boolean tryLock(int seatId, long timeoutMs) {
        ReentrantLock lock = getLock(seatId);
        try {
            boolean acquired = lock.tryLock(timeoutMs, TimeUnit.MILLISECONDS);
            if (!acquired) {
                timeoutCount.incrementAndGet();
                log.debug("Lock timeout for seatId={} after {}ms", seatId, timeoutMs);
            }
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Thread interrupted while waiting for lock on seatId={}", seatId);
            return false;
        }
    }

    /**
     * Releases the lock for {@code seatId} <b>only if held by the current thread</b>.
     *
     * <p><b>Always call from a {@code finally} block</b> — forgetting to unlock
     * causes every subsequent thread trying to book that seat to block indefinitely.
     */
    public void unlock(int seatId) {
        ReentrantLock lock = lockMap.get(seatId);
        if (lock != null && lock.isHeldByCurrentThread()) {
            lock.unlock();
            log.debug("Lock released for seatId={}", seatId);
        }
    }

    /**
     * Total number of {@link #tryLock} calls that timed out.
     * A high value under normal load suggests the lock timeout is too short
     * or the worker-thread count is too low relative to request rate.
     */
    public int getTimeoutCount() {
        return timeoutCount.get();
    }

    /** Number of distinct seat locks currently managed. */
    public int getManagedLockCount() {
        return lockMap.size();
    }
}
