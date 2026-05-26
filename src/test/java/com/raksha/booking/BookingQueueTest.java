package com.raksha.booking;

import com.raksha.booking.lock.BookingQueue;
import com.raksha.booking.model.BookingRequest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BookingQueueTest {

    @Test
    void enqueueAndDequeue_singleThread() throws InterruptedException {
        BookingQueue queue = new BookingQueue(10);
        BookingRequest req = new BookingRequest(1, 1, "A1");
        queue.enqueue(req);
        assertEquals(1, queue.size());
        assertEquals(req, queue.dequeue());
        assertEquals(0, queue.size());
    }

    @Test
    void queue_blocksProducerWhenFull() throws InterruptedException {
        BookingQueue queue = new BookingQueue(1);
        queue.enqueue(new BookingRequest(1, 1, "A1")); // fills the queue

        AtomicInteger enqueued = new AtomicInteger(0);
        Thread producer = new Thread(() -> {
            try {
                queue.enqueue(new BookingRequest(2, 1, "A2")); // should block
                enqueued.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        producer.start();
        Thread.sleep(200); // give time to block
        assertEquals(0, enqueued.get(), "Producer should be blocked");

        queue.dequeue(); // free a slot
        producer.join(1_000);
        assertEquals(1, enqueued.get(), "Producer should have unblocked");
    }

    @Test
    void shutdown_wakesBlockedConsumer() throws InterruptedException {
        BookingQueue queue = new BookingQueue(10); // empty
        CountDownLatch done = new CountDownLatch(1);

        Thread consumer = new Thread(() -> {
            try {
                BookingRequest result = queue.dequeue(); // blocks — queue empty
                assertNull(result, "Shutdown should return null poison pill");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        });
        consumer.start();
        Thread.sleep(100);
        queue.shutdown();
        assertTrue(done.await(1, java.util.concurrent.TimeUnit.SECONDS));
    }

    @Test
    void concurrentProducersAndConsumers_noItemsLost() throws InterruptedException {
        final int ITEMS = 500;
        BookingQueue queue = new BookingQueue(50);
        AtomicInteger consumed = new AtomicInteger(0);
        CountDownLatch producersDone = new CountDownLatch(ITEMS);
        CountDownLatch consumersDone = new CountDownLatch(ITEMS);

        // 10 consumers
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                for (;;) {
                    try {
                        BookingRequest r = queue.dequeue();
                        if (r == null) break;
                        consumed.incrementAndGet();
                        consumersDone.countDown();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }).start();
        }

        // ITEMS producers
        for (int i = 0; i < ITEMS; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    queue.enqueue(new BookingRequest(idx + 1, 1, "A" + (idx % 100 + 1)));
                    producersDone.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }

        producersDone.await();
        queue.shutdown();
        consumersDone.await();

        assertEquals(ITEMS, consumed.get(), "All items should be consumed");
    }
}
