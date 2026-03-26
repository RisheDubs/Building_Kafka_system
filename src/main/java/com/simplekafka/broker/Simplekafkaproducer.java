package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.logging.Logger;
import java.util.Collections;

/**
 * WHAT IS THIS?
 *
 * SimpleKafkaProducer is the high-level producer API described in Stage 7.
 * It wraps SimpleKafkaClient and adds two things the raw client doesn't have:
 *
 *   1. STRING SERIALIZATION
 *      The raw client works with byte[]. This class converts Strings to
 *      UTF-8 bytes so callers don't have to think about encoding.
 *
 *   2. AUTOMATIC PARTITION SELECTION
 *      Two send() variants:
 *        send(message)            → picks a RANDOM partition
 *        send(message, partition) → sends to a SPECIFIC partition
 *
 * WHY TWO SEND VARIANTS?
 *
 *   - Random partition: good for load balancing. Messages spread evenly.
 *     Use this when order between messages doesn't matter.
 *
 *   - Specific partition: good for ordering. All messages for the same
 *     customer/order/key go to the same partition, guaranteeing order.
 *     In real Kafka, you'd hash a key to get the partition number.
 *
 * LAYERED DESIGN:
 *
 *   SimpleKafkaClient      ← raw bytes over TCP (Stage 6)
 *         ^
 *   SimpleKafkaProducer    ← strings, partition selection (Stage 7, this file)
 *         ^
 *   Your application code  ← just calls producer.send("hello")
 */
public class SimpleKafkaProducer {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaProducer.class.getName());

    // ─────────────────────────────────────────────
    // FIELDS
    // ─────────────────────────────────────────────

    /** The underlying raw client — does the actual TCP work */
    private final SimpleKafkaClient client;

    /** Topic this producer writes to — set once at construction */
    private final String topic;

    /** Used for random partition selection in send(String message) */
    private final Random random;

    // ─────────────────────────────────────────────
    // CONSTRUCTOR
    // ─────────────────────────────────────────────

    /**
     * Creates a producer for a single topic.
     *
     * NOTE: Unlike the consumer, the producer doesn't take a partition here —
     * it selects partitions dynamically on each send() call.
     *
     * @param bootstrapBroker  hostname of any broker in the cluster
     * @param bootstrapPort    port of that broker
     * @param topic            the topic to produce messages to
     */
    public SimpleKafkaProducer(String bootstrapBroker, int bootstrapPort, String topic) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic  = topic;
        this.random = new Random();
    }

    /**
     * Exposes the topic metadata cache so higher-level classes
     * (like SimpleKafkaProducer) can read partition counts.
     *
     * Returns an unmodifiable view so callers can't accidentally
     * corrupt the cache.
    */
    public Map<String, TopicMetadata> getTopicMetadataCache() {
        return Collections.unmodifiableMap(topicMetadataCache);
    }

    // ─────────────────────────────────────────────
    // LIFECYCLE
    // ─────────────────────────────────────────────

    /**
     * Connects the underlying client and loads metadata.
     * Must be called before send().
     */
    public void initialize() throws IOException {
        client.initialize();
        LOGGER.info("Producer initialized for topic: " + topic);
    }

    // ─────────────────────────────────────────────
    // SEND TO RANDOM PARTITION
    // ─────────────────────────────────────────────

    /**
     * Sends a message to a randomly selected partition.
     *
     * STEP BY STEP (as described in the blog):
     *   1. Retrieve metadata for the topic (from client's cache or the broker)
     *   2. Verify the topic exists — throw IOException if not
     *   3. Calculate the number of partitions available
     *   4. Select a random partition
     *   5. Call the partition-specific send method
     *
     * WHY RANDOM (not round-robin)?
     * Both achieve load balancing. Random is simpler to implement.
     * Round-robin (using AtomicInteger) is slightly more even over time.
     * Real Kafka uses "sticky partitioning" — picks one partition and
     * sticks with it until a batch is full, then switches. Reduces latency.
     *
     * @param message  the string message to send
     * @return         the offset where this message was stored
     * @throws IOException if the topic doesn't exist or send fails
     */
    public long send(String message) throws IOException {
        // Step 1 & 2: Get topic metadata (throws if topic unknown)
        SimpleKafkaClient.TopicMetadata topicMeta = getTopicMetadataOrThrow();

        // Step 3: How many partitions does this topic have?
        int numPartitions = topicMeta.partitionCount();

        // Step 4: Pick a random partition
        int partition = random.nextInt(numPartitions);

        // Step 5: Delegate to the specific-partition send
        return send(message, partition);
    }

    // ─────────────────────────────────────────────
    // SEND TO SPECIFIC PARTITION
    // ─────────────────────────────────────────────

    /**
     * Sends a message to a specific partition.
     *
     * STEP BY STEP (as described in the blog):
     *   1. Convert the message string to UTF-8 bytes
     *   2. Use the client to send the data to the specified topic and partition
     *   3. Return the offset where the message was written
     *
     * USE THIS WHEN:
     *   - You want all messages for a specific key/user/order on the same partition
     *   - You need ordering guarantees (partition order is guaranteed in Kafka)
     *   - You're manually implementing key-based routing:
     *       int partition = Math.abs(key.hashCode()) % numPartitions;
     *       producer.send(message, partition);
     *
     * @param message    the string message to send
     * @param partition  which partition to write to
     * @return           the offset where this message was stored
     */
    public long send(String message, int partition) throws IOException {
        // Step 1: String → UTF-8 bytes
        // Always use UTF-8 explicitly — default charset varies by JVM/OS
        byte[] data = message.getBytes(StandardCharsets.UTF_8);

        // Step 2: Send via the raw client
        long offset = client.send(topic, partition, data);

        // Step 3: Return the assigned offset
        LOGGER.info("Sent to " + topic + "-" + partition + " @ offset " + offset);
        return offset;
    }

    // ─────────────────────────────────────────────
    // CLOSE
    // ─────────────────────────────────────────────

    /**
     * Releases resources held by this producer.
     *
     * In this implementation there's nothing to close explicitly
     * (connections are opened/closed per-request in SimpleKafkaClient).
     *
     * In a production implementation this would:
     *   - Flush any buffered/batched messages
     *   - Close persistent connections to brokers
     *   - Shut down background threads (e.g. a batching thread)
     *
     * It's still good practice to call close() — future improvements
     * to the client won't require changes to calling code.
     */
    public void close() {
        LOGGER.info("Producer closed for topic: " + topic);
        // Future: flush batches, close connections
    }

    // ─────────────────────────────────────────────
    // PRIVATE HELPERS
    // ─────────────────────────────────────────────

    /**
     * Gets cached topic metadata from the client, or fetches it from the broker.
     * Throws IOException if the topic doesn't exist anywhere.
     *
     * We fetch partition 0's metadata first — this also populates the cache
     * for the topic so partitionCount() works.
     */
    private SimpleKafkaClient.TopicMetadata getTopicMetadataOrThrow() throws IOException {
        // Ensure partition 0 metadata is loaded (which also caches the topic)
        client.fetchMetadata(topic, 0);

        SimpleKafkaClient.TopicMetadata topicMeta =
                client.getTopicMetadataCache().get(topic);

        if (topicMeta == null) {
            throw new IOException("Topic not found: " + topic
                    + ". Create it first with client.createTopic()");
        }
        return topicMeta;
    }

    // ─────────────────────────────────────────────
    // MAIN — standalone demo
    // ─────────────────────────────────────────────

    /**
     * Usage:
     *   java -cp target/simple-kafka-1.0-SNAPSHOT.jar \
     *        com.simplekafka.client.SimpleKafkaProducer \
     *        localhost 9091 test-topic
     */
    public static void main(String[] args) throws Exception {
        String host  = args.length > 0 ? args[0] : "localhost";
        int    port  = args.length > 1 ? Integer.parseInt(args[1]) : 9091;
        String topic = args.length > 2 ? args[2] : "test-topic";

        System.out.println("=== SimpleKafkaProducer ===");
        System.out.println("Broker: " + host + ":" + port + "  Topic: " + topic);

        SimpleKafkaProducer producer = new SimpleKafkaProducer(host, port, topic);
        producer.initialize();

        // Send 10 messages — partition chosen randomly each time
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            long offset = producer.send(message);
            System.out.println("[SENT] \"" + message + "\" -> offset " + offset);
            Thread.sleep(500);
        }

        producer.close();
        System.out.println("Done.");
    }
}