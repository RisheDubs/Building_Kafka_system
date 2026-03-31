package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;        // ← was missing
import java.util.Random;
import java.util.logging.Logger;

/**
 * High-level producer API (Stage 7).
 * Wraps SimpleKafkaClient and adds:
 *   1. String -> UTF-8 byte serialisation
 *   2. Two send() variants: random partition and specific partition
 */
public class SimpleKafkaProducer {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaProducer.class.getName());

    private static final int   DEFAULT_NUM_PARTITIONS     = 3;
    private static final short DEFAULT_REPLICATION_FACTOR = 1;

    // ─────────────────────────────────────────────
    // FIELDS
    // ─────────────────────────────────────────────

    private final SimpleKafkaClient client;
    private final String topic;
    private final Random random;

    // ─────────────────────────────────────────────
    // CONSTRUCTOR
    // ─────────────────────────────────────────────

    public SimpleKafkaProducer(String bootstrapBroker, int bootstrapPort, String topic) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic  = topic;
        this.random = new Random();
    }

    // ─────────────────────────────────────────────
    // LIFECYCLE
    // ─────────────────────────────────────────────

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
     * Steps (per Stage 7 blog):
     *   1. Retrieve metadata for the topic
     *   2. Verify topic exists — throw IOException if not
     *   3. Count available partitions
     *   4. Pick a random partition
     *   5. Delegate to send(message, partition)
     */
    public long send(String message) throws IOException {
        // Steps 1 & 2: fetch/validate metadata
        SimpleKafkaClient.TopicMetadata topicMeta = getTopicMetadataOrThrow();

        // Step 3: count partitions
        int numPartitions = topicMeta.partitionCount();

        // Step 4: pick random partition
        int partition = random.nextInt(numPartitions);

        // Step 5: delegate
        return send(message, partition);
    }

    // ─────────────────────────────────────────────
    // SEND TO SPECIFIC PARTITION
    // ─────────────────────────────────────────────

    /**
     * Sends a message to a specific partition.
     *
     * Steps (per Stage 7 blog):
     *   1. Convert String -> UTF-8 bytes
     *   2. Call client.send(topic, partition, bytes)
     *   3. Return the offset
     */
    public long send(String message, int partition) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        long offset = client.send(topic, partition, data);
        LOGGER.info("Sent to " + topic + "-" + partition + " @ offset " + offset);
        return offset;
    }

    // ─────────────────────────────────────────────
    // CLOSE
    // ─────────────────────────────────────────────

    /**
     * Releases resources. No-op here but good practice to call —
     * a production version would flush buffered messages and close connections.
     */
    public void close() {
        LOGGER.info("Producer closed for topic: " + topic);
    }

    // ─────────────────────────────────────────────
    // PRIVATE HELPERS
    // ─────────────────────────────────────────────

    /**
     * Fetches partition 0 metadata (which populates the topic cache),
     * then reads the cached TopicMetadata. Throws if the topic is unknown.
     *
     * NOTE: getTopicMetadataCache() is defined on SimpleKafkaClient — NOT here.
     * The misplaced version in your file that referenced topicMetadataCache
     * directly was causing the compile errors.
     */
    private SimpleKafkaClient.TopicMetadata getTopicMetadataOrThrow() throws IOException {
        client.fetchMetadata(topic, 0);

        // getTopicMetadataCache() lives on SimpleKafkaClient — that's correct
        Map<String, SimpleKafkaClient.TopicMetadata> cache =
                client.getTopicMetadataCache();

        SimpleKafkaClient.TopicMetadata topicMeta = cache.get(topic);

        if (topicMeta == null) {
            throw new IOException("Topic not found: " + topic
                    + ". Create it first with client.createTopic()");
        }
        return topicMeta;
    }

    // ─────────────────────────────────────────────
    // MAIN
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

        // Create topic before producing
        producer.client.createTopic(topic, DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR);

        // Give the broker time to register the topic before fetching metadata
        Thread.sleep(2000);

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