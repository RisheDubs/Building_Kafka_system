package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WHAT IS THIS?
 *
 * SimpleKafkaConsumer is the high-level consumer API described in Stage 7.
 * It wraps SimpleKafkaClient and adds:
 *
 *   1. AUTOMATIC OFFSET MANAGEMENT
 *      Tracks currentOffset internally. After each poll, it advances
 *      automatically so you never re-read the same messages.
 *
 *   2. BACKGROUND THREAD CONSUMPTION (startConsuming / stopConsuming)
 *      The key design in Stage 7: the poll loop runs on a DEDICATED THREAD.
 *      You call startConsuming(handler) and your callback fires for every message.
 *      You don't block — your main thread stays free.
 *
 *   3. THREAD-SAFE START/STOP
 *      Uses AtomicBoolean (compareAndSet) to ensure startConsuming() can only
 *      be called once and stopConsuming() is safe to call from any thread.
 *
 *   4. SEEK / OFFSET CONTROL
 *      seek(offset) lets you jump to any point in the partition — replay
 *      from 0, skip ahead, etc.
 *
 * THE KEY DESIGN DIFFERENCE FROM MY EARLIER VERSION:
 *
 * My previous SimpleKafkaConsumer had startPollLoop() which BLOCKED the
 * caller's thread. The blog's design uses a background daemon thread instead:
 *
 *   consumer.startConsuming(handler);   // returns immediately
 *   // ... your code keeps running here ...
 *   consumer.stopConsuming();           // shuts down the background thread
 *
 * This is much more useful in real applications where you want to produce
 * AND consume at the same time, or consume from multiple partitions in parallel.
 *
 * DAEMON THREAD:
 * The consumer thread is set as a daemon thread (consumerThread.setDaemon(true)).
 * Daemon threads don't prevent the JVM from shutting down. If main() exits,
 * the JVM doesn't wait for the consumer thread to finish — it just dies.
 * This is intentional: if your app shuts down, you don't want stray threads
 * keeping the process alive.
 *
 * ATOMIC BOOLEAN (compareAndSet):
 * running.compareAndSet(false, true) means:
 *   "If running is currently false, set it to true and return true"
 *   "If running is already true, do nothing and return false"
 * This is atomic — thread-safe without needing synchronized{}.
 * It prevents two threads from both "winning" a race to start the consumer.
 */
public class SimpleKafkaConsumer {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());

    // ─────────────────────────────────────────────
    // CONSTANTS
    // ─────────────────────────────────────────────

    /** Max bytes per fetch — limits how many messages come back in one call */
    private static final int MAX_BYTES = 1024 * 1024; // 1 MB

    /**
     * How long to wait when poll() returns empty before polling again.
     *
     * WHY THIS MATTERS:
     * Without a wait, an idle consumer would call the broker thousands of
     * times per second, wasting CPU and network bandwidth.
     * With 500ms: at most 2 polls per second when quiet.
     * When messages are flowing, the wait doesn't apply.
     */
    private static final long POLL_INTERVAL_MS = 500;

    // ─────────────────────────────────────────────
    // FIELDS
    // ─────────────────────────────────────────────

    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;

    /**
     * Tracks where we are in the partition.
     * After poll() returns N messages, currentOffset advances by N.
     * This value is NOT thread-safe for reads — only the consumer thread writes it.
     */
    private long currentOffset;

    /**
     * AtomicBoolean: thread-safe boolean.
     * true  = consumer loop is running
     * false = consumer is stopped
     *
     * We use this instead of a plain boolean because startConsuming() and
     * stopConsuming() may be called from different threads.
     */
    private final AtomicBoolean running;

    /**
     * The background thread that runs the poll loop.
     * Stored so stopConsuming() can interrupt and join it.
     */
    private Thread consumerThread;

    // ─────────────────────────────────────────────
    // CONSTRUCTOR
    // ─────────────────────────────────────────────

    /**
     * Creates a consumer for a specific topic + partition.
     *
     * Unlike the producer, the consumer is tied to ONE partition.
     * To consume from multiple partitions, create multiple consumer instances.
     * (Real Kafka handles multi-partition assignment via consumer groups.)
     *
     * @param bootstrapBroker  hostname of any broker in the cluster
     * @param bootstrapPort    port of that broker
     * @param topic            the topic to consume from
     * @param partition        which partition to read (0, 1, 2...)
     */
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort,
                                String topic, int partition) {
        this.client        = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic         = topic;
        this.partition     = partition;
        this.currentOffset = 0;       // start from the beginning by default
        this.running       = new AtomicBoolean(false);
    }

    // ─────────────────────────────────────────────
    // LIFECYCLE
    // ─────────────────────────────────────────────

    /**
     * Connects the underlying client and loads metadata.
     * Must be called before poll() or startConsuming().
     */
    public void initialize() throws IOException {
        client.initialize();
        LOGGER.info("Consumer initialized for " + topic + "-" + partition);
    }

    // ─────────────────────────────────────────────
    // POLL (single fetch — called internally by the loop)
    // ─────────────────────────────────────────────

    /**
     * Fetches the next batch of messages from the broker.
     *
     * STEP BY STEP:
     *   1. Fetch messages from topic/partition starting at currentOffset
     *   2. Limit response to MAX_BYTES
     *   3. If messages were returned, advance currentOffset by the count
     *   4. Return the messages
     *
     * You can call this directly (manual polling) or use startConsuming()
     * for automatic background polling.
     *
     * @return list of raw message bytes (may be empty if no new messages)
     */
    public List<byte[]> poll() throws IOException {
        List<byte[]> messages = client.fetch(topic, partition, currentOffset, MAX_BYTES);

        if (!messages.isEmpty()) {
            // Advance offset AFTER reading — "at-least-once" semantics
            // If we crash here, we'll re-read these messages on restart.
            // "Exactly-once" would require a two-phase commit — much harder.
            currentOffset += messages.size();
        }

        return messages;
    }

    // ─────────────────────────────────────────────
    // START CONSUMING (background thread model)
    // ─────────────────────────────────────────────

    /**
     * Starts a background thread that continuously polls for messages and
     * calls handler.handle() for each one.
     *
     * Returns immediately — your calling code is not blocked.
     *
     * HOW IT WORKS:
     *
     *   compareAndSet(false, true) — atomic "if stopped, then start"
     *       Only one thread can win this check. If startConsuming() is
     *       called twice, the second call does nothing.
     *
     *   The loop:
     *       while (running.get()) {
     *           poll() → for each message → handler.handle(message, offset)
     *           if empty → sleep(POLL_INTERVAL_MS)
     *       }
     *
     *   Daemon thread:
     *       setDaemon(true) so the JVM can exit even if this thread is running.
     *
     * MESSAGE HANDLER:
     *   The handler receives each message as bytes AND its offset.
     *   The offset is: (currentOffset after poll) - (total messages) + (index of this message)
     *   e.g. after reading messages at offsets 5,6,7:
     *     currentOffset = 8, messages.size() = 3
     *     message[0] offset = 8 - 3 + 0 = 5 ✓
     *     message[1] offset = 8 - 3 + 1 = 6 ✓
     *     message[2] offset = 8 - 3 + 2 = 7 ✓
     *
     * @param handler  callback that processes each message
     */
    public void startConsuming(MessageHandler handler) {
        // compareAndSet(expectedValue, newValue):
        // "If running is false, set it to true and proceed"
        // Returns false (and does nothing) if running was already true
        if (running.compareAndSet(false, true)) {

            consumerThread = new Thread(() -> {
                try {
                    while (running.get()) {
                        List<byte[]> messages = poll();

                        // Pass each message to the handler with its offset
                        for (byte[] message : messages) {
                            // Calculate this specific message's offset:
                            // currentOffset already advanced past all messages,
                            // so we work backwards using indexOf
                            long messageOffset = currentOffset
                                    - messages.size()
                                    + messages.indexOf(message);

                            handler.handle(message, messageOffset);
                        }

                        // No messages available — back off before next poll
                        if (messages.isEmpty()) {
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    }
                } catch (InterruptedException e) {
                    // stopConsuming() interrupted us — clean exit
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    if (running.get()) {
                        // Only log if we weren't intentionally stopped
                        LOGGER.log(Level.SEVERE, "Error in consumer loop", e);
                    }
                    running.set(false);
                }
            });

            // Daemon = JVM won't wait for this thread to finish on shutdown
            consumerThread.setDaemon(true);
            consumerThread.start();

            LOGGER.info("Started consuming from topic: " + topic
                    + ", partition: " + partition);
        }
    }

    // ─────────────────────────────────────────────
    // STOP CONSUMING
    // ─────────────────────────────────────────────

    /**
     * Stops the background consumer thread.
     *
     * STEP BY STEP:
     *   1. compareAndSet(true, false) — atomic "if running, then stop"
     *      Only works once — calling stop twice is safe and does nothing.
     *
     *   2. consumerThread.interrupt()
     *      Wakes the thread if it's sleeping in Thread.sleep(POLL_INTERVAL_MS).
     *      Without this, a quiet consumer might sleep for 500ms after stop().
     *
     *   3. consumerThread.join(1000)
     *      Wait UP TO 1 second for the thread to actually finish.
     *      This ensures resources are cleaned up before continuing.
     *
     *   4. Preserve interrupted status
     *      If join() itself was interrupted, we re-set the interrupt flag
     *      so the calling code can handle it.
     */
    public void stopConsuming() {
        if (running.compareAndSet(true, false)) {
            if (consumerThread != null) {
                try {
                    consumerThread.interrupt();           // wake it up if sleeping
                    consumerThread.join(1000);            // wait up to 1 second
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();  // preserve interrupted status
                }
            }
            LOGGER.info("Stopped consuming from topic: " + topic
                    + ", partition: " + partition);
        }
    }

    // ─────────────────────────────────────────────
    // OFFSET MANAGEMENT
    // ─────────────────────────────────────────────

    /**
     * Jumps the consumer to a specific offset.
     *
     * Use cases:
     *   seek(0)                → replay all messages from the beginning
     *   seek(lastCommitted)    → recover from a crash
     *   seek(latestOffset)     → skip all old messages, only read new ones
     *
     * NOTE: Call this before startConsuming() or between stopConsuming()
     * and startConsuming(). Calling while the loop is running is technically
     * possible but creates a race condition.
     *
     * @param offset  the offset to seek to
     */
    public void seek(long offset) {
        this.currentOffset = offset;
        LOGGER.info("Seeked to offset " + offset
                + " on " + topic + "-" + partition);
    }

    /**
     * Returns the current consumer offset.
     *
     * This is the offset of the NEXT message to be fetched — not the
     * last message that was processed. If you've consumed offsets 0,1,2,
     * getCurrentOffset() returns 3.
     *
     * Useful for:
     *   - Checkpointing (saving offset to a database for crash recovery)
     *   - Monitoring consumer lag: brokerLatestOffset - getCurrentOffset()
     */
    public long getCurrentOffset() {
        return currentOffset;
    }

    // ─────────────────────────────────────────────
    // FUNCTIONAL INTERFACE
    // ─────────────────────────────────────────────

    /**
     * Callback interface for processing individual messages.
     *
     * Unlike my previous version which only passed bytes, the blog's version
     * also passes the offset. This is important because:
     *   - You might want to log which offset had a problem
     *   - You might want to commit the offset to a database for crash recovery
     *   - You might want to print "consumed offset X" for debugging
     *
     * Usage with lambda:
     *   consumer.startConsuming((message, offset) -> {
     *       String text = new String(message, StandardCharsets.UTF_8);
     *       System.out.println("offset=" + offset + " msg=" + text);
     *   });
     */
    @FunctionalInterface
    public interface MessageHandler {
        /**
         * @param message  raw bytes of the message
         * @param offset   the Kafka offset of this specific message
         */
        void handle(byte[] message, long offset) throws Exception;
    }

    // ─────────────────────────────────────────────
    // MAIN — standalone demo
    // ─────────────────────────────────────────────

    /**
     * Usage:
     *   java -cp target/simple-kafka-1.0-SNAPSHOT.jar \
     *        com.simplekafka.client.SimpleKafkaConsumer \
     *        localhost 9091 test-topic 0
     *
     * Args: brokerHost brokerPort topicName partitionId
     */
    public static void main(String[] args) throws Exception {
        String host      = args.length > 0 ? args[0] : "localhost";
        int    port      = args.length > 1 ? Integer.parseInt(args[1]) : 9091;
        String topic     = args.length > 2 ? args[2] : "test-topic";
        int    partition = args.length > 3 ? Integer.parseInt(args[3]) : 0;

        System.out.println("=== SimpleKafkaConsumer ===");
        System.out.println("Broker: " + host + ":" + port
                + "  Topic: " + topic + "  Partition: " + partition);
        System.out.println("Press Ctrl+C to stop.\n");

        SimpleKafkaConsumer consumer =
                new SimpleKafkaConsumer(host, port, topic, partition);
        consumer.initialize();

        // Shutdown hook to stop cleanly on Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            consumer.stopConsuming();
        }));

        // Start background consumption — this call returns immediately
        consumer.startConsuming((message, offset) -> {
            String text = new String(message, StandardCharsets.UTF_8);
            System.out.println("[RECEIVED] offset=" + offset + " | \"" + text + "\"");
        });

        // Main thread stays alive — the consumer runs in the background
        // In a real app you'd be doing other work here
        Thread.currentThread().join();
    }
}