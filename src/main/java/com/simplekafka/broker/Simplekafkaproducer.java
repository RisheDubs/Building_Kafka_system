package com.simplekafka.client;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WHAT IS THIS?
 *
 * SimpleKafkaProducer is the high-level producer API — the thing a real
 * application would use. It sits on top of SimpleKafkaClient and adds:
 *
 *   1. AUTOMATIC PARTITION SELECTION (round-robin)
 *      You just say "send this to topic X" — the producer decides which
 *      partition. Real Kafka does the same unless you specify a key.
 *
 *   2. TOPIC AUTO-CREATION
 *      If the topic doesn't exist yet, the producer creates it.
 *      Real Kafka has a config for this: auto.create.topics.enable
 *
 *   3. SEND LOOP WITH RETRY
 *      Wraps the raw send() in error handling so one bad message
 *      doesn't crash the whole producer.
 *
 *   4. RUNNABLE AS A STANDALONE PROCESS
 *      main() reads from args so you can run it from the terminal:
 *      java ... SimpleKafkaProducer localhost 9091 test-topic
 *
 * RELATIONSHIP TO SIMPLEKAFKACLIENT:
 *
 *   SimpleKafkaClient    <- raw protocol layer (bytes in, bytes out)
 *         ^
 *   SimpleKafkaProducer  <- this class, friendly API layer
 *
 * PARTITION SELECTION:
 *
 * Real Kafka uses a partitioner to decide which partition a message goes to:
 *   - If message has a KEY -> hash(key) % numPartitions  (same key -> same partition)
 *   - If no key            -> round-robin across partitions
 *
 * We don't implement keys here, so we use round-robin with an AtomicInteger
 * counter that increments on every send.
 *
 * AtomicInteger is thread-safe — if you ever send from multiple threads,
 * the counter won't get corrupted.
 */
public class SimpleKafkaProducer {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaProducer.class.getName());

    // ─────────────────────────────────────────────
    // CONSTANTS
    // ─────────────────────────────────────────────

    /** Default number of partitions when auto-creating a topic */
    private static final int DEFAULT_NUM_PARTITIONS = 3;

    /** Default replication factor when auto-creating a topic */
    private static final short DEFAULT_REPLICATION_FACTOR = 1;

    /** How long to wait between messages in the demo send loop (ms) */
    private static final int SEND_INTERVAL_MS = 1000;

    // ─────────────────────────────────────────────
    // FIELDS
    // ─────────────────────────────────────────────

    private final SimpleKafkaClient client;
    private final String topicName;
    private final int numPartitions;

    /**
     * Round-robin partition counter.
     * send() does: partitionId = counter.getAndIncrement() % numPartitions
     *
     * e.g. with 3 partitions: 0, 1, 2, 0, 1, 2, 0, 1, 2 ...
     * This spreads messages evenly across all partitions.
     */
    private final AtomicInteger partitionCounter;

    // ─────────────────────────────────────────────
    // CONSTRUCTOR
    // ─────────────────────────────────────────────

    /**
     * @param brokerHost    bootstrap broker hostname
     * @param brokerPort    bootstrap broker port
     * @param topicName     topic to produce to (will be auto-created if missing)
     * @param numPartitions how many partitions to create if auto-creating
     */
    public SimpleKafkaProducer(String brokerHost, int brokerPort,
                                String topicName, int numPartitions) throws Exception {
        this.client           = new SimpleKafkaClient(brokerHost, brokerPort);
        this.topicName        = topicName;
        this.numPartitions    = numPartitions;
        this.partitionCounter = new AtomicInteger(0);

        // Initialize the underlying client (loads metadata)
        client.initialize();
    }

    // ─────────────────────────────────────────────
    // START
    // ─────────────────────────────────────────────

    /**
     * Sets up the topic and prepares to produce.
     * Ensures the topic exists before we try to send anything.
     */
    public void start() throws Exception {
        ensureTopicExists();
    }

    /**
     * Creates the topic if it doesn't already exist.
     *
     * We use try/catch here because the broker logs a warning
     * (not an exception) if the topic already exists.
     */
    private void ensureTopicExists() {
        try {
            client.createTopic(topicName, numPartitions, DEFAULT_REPLICATION_FACTOR);
            LOGGER.info("Topic ready: " + topicName
                    + " (" + numPartitions + " partitions)");
        } catch (Exception e) {
            // Topic might already exist — that's fine
            LOGGER.info("Topic may already exist: " + topicName + " — " + e.getMessage());
        }
    }

    // ─────────────────────────────────────────────
    // SEND
    // ─────────────────────────────────────────────

    /**
     * Sends a single message to the topic.
     * Automatically selects the partition using round-robin.
     *
     * @param message  raw bytes to send
     * @return         the offset assigned to this message, or -1 on failure
     */
    public long send(byte[] message) {
        // Pick the next partition in round-robin order
        int partitionId = partitionCounter.getAndIncrement() % numPartitions;

        try {
            long offset = client.send(topicName, partitionId, message);
            LOGGER.info("Sent to " + topicName + "-" + partitionId
                    + " @ offset " + offset
                    + " | msg: " + new String(message));
            return offset;

        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "Failed to send to " + topicName + "-" + partitionId, e);
            return -1;
        }
    }

    /** Convenience overload — send a String message */
    public long send(String message) {
        return send(message.getBytes());
    }

    // ─────────────────────────────────────────────
    // DEMO: CONTINUOUS SEND LOOP
    // ─────────────────────────────────────────────

    /**
     * Sends numbered messages in a loop until interrupted.
     * Used for manual testing — you can watch offsets climb in real time.
     *
     * In a real application you'd call send(message) directly from your
     * business logic instead of using this loop.
     */
    public void startSendLoop() throws InterruptedException {
        System.out.println("Starting producer loop for topic: " + topicName);
        System.out.println("Press Ctrl+C to stop.\n");

        int messageNumber = 0;

        while (!Thread.currentThread().isInterrupted()) {
            String payload = "message-" + messageNumber;
            long offset = send(payload);

            if (offset >= 0) {
                System.out.println("[SENT] #" + messageNumber
                        + " -> partition " + (messageNumber % numPartitions)
                        + " @ offset " + offset
                        + " | \"" + payload + "\"");
            } else {
                System.out.println("[FAIL] #" + messageNumber + " — send failed");
            }

            messageNumber++;
            Thread.sleep(SEND_INTERVAL_MS);
        }
    }

    // ─────────────────────────────────────────────
    // MAIN — run as a standalone process
    // ─────────────────────────────────────────────

    /**
     * Usage:
     *   java -cp target/simple-kafka-1.0-SNAPSHOT.jar \
     *        com.simplekafka.client.SimpleKafkaProducer \
     *        localhost 9091 test-topic
     */
    public static void main(String[] args) {
        String host  = args.length > 0 ? args[0] : "localhost";
        int    port  = args.length > 1 ? Integer.parseInt(args[1]) : 9091;
        String topic = args.length > 2 ? args[2] : "test-topic";

        System.out.println("SimpleKafkaProducer starting...");
        System.out.println("  Broker    : " + host + ":" + port);
        System.out.println("  Topic     : " + topic);
        System.out.println("  Partitions: " + DEFAULT_NUM_PARTITIONS);

        try {
            SimpleKafkaProducer producer = new SimpleKafkaProducer(
                    host, port, topic, DEFAULT_NUM_PARTITIONS);

            producer.start();

            // Clean shutdown on Ctrl+C
            Runtime.getRuntime().addShutdownHook(new Thread(() ->
                    System.out.println("\nShutting down producer...")));

            producer.startSendLoop();

        } catch (Exception e) {
            System.err.println("Producer failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}