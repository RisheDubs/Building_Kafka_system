package com.simplekafka.client;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WHAT IS THIS?
 *
 * SimpleKafkaConsumer is the high-level consumer API — the thing a real
 * application would use to read messages from a topic. It sits on top of
 * SimpleKafkaClient and adds:
 *
 *   1. AUTOMATIC OFFSET TRACKING
 *      The consumer remembers where it left off. After each successful
 *      poll, it advances its internal offset so it never re-reads the
 *      same messages (unless you reset it).
 *
 *      This is called "at-least-once" delivery semantics — if the process
 *      crashes before committing, it may re-read messages on restart.
 *      Real Kafka has an explicit commitOffset() call for this reason.
 *
 *   2. POLL LOOP
 *      Consumers in Kafka are PULL-based: the consumer asks the broker
 *      for messages on its own schedule, not the other way around.
 *      This is very different from a push model.
 *
 *      WHY PULL? 
 *      The consumer controls its own pace. A slow consumer doesn't get
 *      overwhelmed by a fast producer — it just reads when it's ready.
 *
 *   3. EMPTY POLL BACKOFF
 *      If there are no new messages, we wait a bit before asking again.
 *      Without this, the consumer would spam the broker thousands of
 *      times per second — wasting CPU and network. This is called
 *      "poll backoff" or "consumer wait" in real Kafka.
 *
 *   4. RUNNABLE AS A STANDALONE PROCESS
 *      main() reads from args so you can run it from the terminal:
 *      java ... SimpleKafkaConsumer localhost 9091 test-topic 0
 *      (the 0 is the starting offset — use 0 for "from the beginning")
 *
 * RELATIONSHIP TO SIMPLEKAFKACLIENT:
 *
 *   SimpleKafkaClient    <- raw protocol layer (bytes in, bytes out)
 *         ^
 *   SimpleKafkaConsumer  <- this class, friendly API layer
 *
 * OFFSETS — THE KEY CONCEPT:
 *
 * Every message in a Kafka partition has a unique, sequential offset.
 *   Partition 0:  [offset 0][offset 1][offset 2][offset 3]...
 *
 * The consumer tracks its own offset. It tells the broker: "give me
 * messages starting at offset N." After reading, it sets N = N + count.
 *
 * This means:
 *   - Multiple consumers can read the same partition at different speeds
 *   - Consumers can "rewind" by resetting their offset
 *   - Messages are not deleted when consumed (unlike a queue)
 *
 * Real Kafka stores committed offsets in a special topic: __consumer_offsets
 * We store ours just in memory (lost on restart).
 */
public class SimpleKafkaConsumer {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());

    // ─────────────────────────────────────────────
    // CONSTANTS
    // ─────────────────────────────────────────────

    /** Max bytes to fetch per poll. Controls how many messages come back at once. */
    private static final int MAX_FETCH_BYTES = 1024 * 1024; // 1 MB

    /** Wait this long between polls when no messages are available (ms) */
    private static final int EMPTY_POLL_WAIT_MS = 500;

    /** Wait this long between polls when messages were received (ms) */
    private static final int ACTIVE_POLL_WAIT_MS = 100;

    // ─────────────────────────────────────────────
    // FIELDS
    // ─────────────────────────────────────────────

    private final SimpleKafkaClient client;
    private final String topicName;
    private final int partitionId;

    /**
     * Current consumer offset — the next offset to fetch from.
     *
     * Starts at whatever the caller passed in (usually 0 for "from beginning",
     * or the broker's latest offset for "only new messages").
     *
     * After each batch of messages, we advance this by the number of messages received.
     */
    private long currentOffset;

    private volatile boolean running = false;

    // ─────────────────────────────────────────────
    // CONSTRUCTOR
    // ─────────────────────────────────────────────

    /**
     * @param brokerHost    bootstrap broker hostname
     * @param brokerPort    bootstrap broker port
     * @param topicName     topic to consume from
     * @param partitionId   which partition to read (0, 1, 2, etc.)
     * @param startOffset   which offset to start reading from
     *                        0 = read from the very beginning
     *                       -1 = read only new messages (latest)
     */
    public SimpleKafkaConsumer(String brokerHost, int brokerPort,
                                String topicName, int partitionId,
                                long startOffset) throws Exception {
        this.client      = new SimpleKafkaClient(brokerHost, brokerPort);
        this.topicName   = topicName;
        this.partitionId = partitionId;
        this.currentOffset = startOffset;

        client.initialize();
    }

    // ─────────────────────────────────────────────
    // POLL (single fetch)
    // ─────────────────────────────────────────────

    /**
     * Fetches the next batch of messages from the broker, starting at currentOffset.
     *
     * This is the core of the consumer. In real Kafka, poll() also:
     *   - Sends heartbeats to the group coordinator
     *   - Triggers partition rebalances
     *   - Manages consumer group membership
     *
     * We keep it simple: just fetch and return.
     *
     * @return list of messages (may be empty if no new messages available)
     */
    public List<byte[]> poll() {
        try {
            List<byte[]> messages = client.fetch(
                    topicName, partitionId, currentOffset, MAX_FETCH_BYTES);

            if (!messages.isEmpty()) {
                // Advance offset AFTER successful read
                // This is the "at-least-once" pattern:
                // if we crash here before the next poll, we'll re-read these messages
                currentOffset += messages.size();
            }

            return messages;

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Poll failed at offset " + currentOffset, e);
            return List.of(); // return empty list, not an exception — let the loop retry
        }
    }

    // ─────────────────────────────────────────────
    // CONTINUOUS POLL LOOP
    // ─────────────────────────────────────────────

    /**
     * Runs a continuous poll loop, processing each message as it arrives.
     *
     * The MessageHandler is a functional interface — you pass in a lambda
     * that defines what to DO with each message. This is the same pattern
     * real Kafka uses with its ConsumerRecords API.
     *
     * Example usage:
     *   consumer.startPollLoop(message -> {
     *       String text = new String(message);
     *       System.out.println("Processing: " + text);
     *       // ... your business logic here
     *   });
     *
     * @param handler  called once per message with the raw bytes
     */
    public void startPollLoop(MessageHandler handler) throws InterruptedException {
        running = true;

        System.out.println("Consumer started:");
        System.out.println("  Topic     : " + topicName);
        System.out.println("  Partition : " + partitionId);
        System.out.println("  Start Offset: " + currentOffset);
        System.out.println("Press Ctrl+C to stop.\n");

        while (running && !Thread.currentThread().isInterrupted()) {
            List<byte[]> messages = poll();

            if (messages.isEmpty()) {
                // No new messages — back off so we don't hammer the broker
                Thread.sleep(EMPTY_POLL_WAIT_MS);
            } else {
                // Process each message
                for (byte[] message : messages) {
                    try {
                        handler.handle(message);
                    } catch (Exception e) {
                        // Don't let a handler error stop the consumer
                        LOGGER.log(Level.WARNING, "Message handler threw an exception", e);
                    }
                }
                // Small pause between active polls — prevents CPU spin
                Thread.sleep(ACTIVE_POLL_WAIT_MS);
            }
        }

        System.out.println("Consumer stopped. Final offset: " + currentOffset);
    }

    /** Stops the poll loop gracefully */
    public void stop() {
        running = false;
    }

    /** Current consumer offset (useful for checkpointing) */
    public long getCurrentOffset() {
        return currentOffset;
    }

    /** Manually reset offset — allows "replaying" messages from any point */
    public void seekTo(long offset) {
        this.currentOffset = offset;
        LOGGER.info("Consumer seeked to offset " + offset);
    }

    // ─────────────────────────────────────────────
    // FUNCTIONAL INTERFACE
    // ─────────────────────────────────────────────

    /**
     * Callback interface for processing messages.
     *
     * WHY A FUNCTIONAL INTERFACE?
     * It lets callers use a lambda instead of creating a whole class.
     * This is the same pattern as Java's Runnable, Comparator, etc.
     *
     * In real Kafka, you implement ConsumerRebalanceListener or
     * use the poll loop with records.forEach(...) instead.
     */
    @FunctionalInterface
    public interface MessageHandler {
        void handle(byte[] message) throws Exception;
    }

    // ─────────────────────────────────────────────
    // MAIN — run as a standalone process
    // ─────────────────────────────────────────────

    /**
     * Usage:
     *   java -cp target/simple-kafka-1.0-SNAPSHOT.jar \
     *        com.simplekafka.client.SimpleKafkaConsumer \
     *        localhost 9091 test-topic 0
     *
     * Args:
     *   brokerHost   (default: localhost)
     *   brokerPort   (default: 9091)
     *   topicName    (default: test-topic)
     *   partitionId  (default: 0)
     *   startOffset  (default: 0 = from beginning)
     */
    public static void main(String[] args) {
        String host        = args.length > 0 ? args[0] : "localhost";
        int    port        = args.length > 1 ? Integer.parseInt(args[1]) : 9091;
        String topic       = args.length > 2 ? args[2] : "test-topic";
        int    partitionId = args.length > 3 ? Integer.parseInt(args[3]) : 0;
        long   startOffset = args.length > 4 ? Long.parseLong(args[4])   : 0L;

        System.out.println("SimpleKafkaConsumer starting...");
        System.out.println("  Broker   : " + host + ":" + port);
        System.out.println("  Topic    : " + topic);
        System.out.println("  Partition: " + partitionId);
        System.out.println("  Offset   : " + startOffset);

        try {
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(
                    host, port, topic, partitionId, startOffset);

            // Clean shutdown on Ctrl+C
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down consumer...");
                consumer.stop();
            }));

            // The message handler: just print each message
            // In a real app this is where your business logic goes
            consumer.startPollLoop(message -> {
                String text = new String(message);
                System.out.println("[RECEIVED] offset=" + (consumer.getCurrentOffset() - 1)
                        + " | \"" + text + "\"");
            });

        } catch (Exception e) {
            System.err.println("Consumer failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}