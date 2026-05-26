package com.raksha.booking;

import com.raksha.booking.lock.SeatLockManager;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SeatLockManagerTest {

    @Test
    void tryLock_acquiresAndReleasesLock() {
        SeatLockManager mgr = new SeatLockManager();
        assertTrue(mgr.tryLock(1, 100), "Should acquire free lock");
        mgr.unlock(1);
        // Lock should be available again after unlock
        assertTrue(mgr.tryLock(1, 100), "Should re-acquire after unlock");
        mgr.unlock(1);
    }

    @Test
    void tryLock_timesOutWhenHeldByAnotherThread() throws InterruptedException {
        SeatLockManager mgr = new SeatLockManager();
        CountDownLatch lockHeld    = new CountDownLatch(1);
        CountDownLatch unlockReady = new CountDownLatch(1);

        // Thread A holds the lock
        Thread holder = new Thread(() -> {
            mgr.tryLock(42, 5_000);
            lockHeld.countDown();
            try { unlockReady.await(); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            mgr.unlock(42);
        });
        holder.start();

        lockHeld.await(); // wait until A has the lock

        // Thread B should time out quickly
        boolean acquired = mgr.tryLock(42, 100);
        assertFalse(acquired, "Should time out while seat is locked");
        assertEquals(1, mgr.getTimeoutCount());

        unlockReady.countDown();
        holder.join();
    }

    @Test
    void differentSeats_doNotBlockEachOther() throws InterruptedException {
        SeatLockManager mgr = new SeatLockManager();
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch done = new CountDownLatch(2);

        // Two threads lock different seat IDs simultaneously
        for (int seatId : new int[]{10, 20}) {
            final int id = seatId;
            new Thread(() -> {
                try {
                    if (mgr.tryLock(id, 500)) {
                        count.incrementAndGet();
                        Thread.sleep(100);
                        mgr.unlock(id);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }

        done.await();
        assertEquals(2, count.get(), "Both threads should succeed — different seats");
    }

    @Test
    void unlock_doesNotThrowIfNotHeld() {
        SeatLockManager mgr = new SeatLockManager();
        // Should not throw even if lock was never acquired
        assertDoesNotThrow(() -> mgr.unlock(999));
    }
}
