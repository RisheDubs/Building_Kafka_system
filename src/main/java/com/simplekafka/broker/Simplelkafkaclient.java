package com.simplekafka.client;

import com.simplekafka.broker.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WHAT IS THIS CLASS?
 *
 * Up until now everything we've built is SERVER-SIDE (the broker).
 * This class is the CLIENT — the code that a producer or consumer application
 * would use to talk to the broker.
 *
 * WHY DO WE NEED A CLIENT LIBRARY?
 *
 * A producer shouldn't have to know:
 *   - Which broker is the leader for partition 2 of "orders"?
 *   - What binary format does the broker expect?
 *   - What happens if the leader changes mid-flight?
 *
 * The client library hides ALL of that. The user just calls:
 *   client.send("orders", 0, "hello".getBytes());
 *
 * And the library handles the rest. This is exactly what the real Kafka
 * Java client does — it's a thick client with built-in broker discovery,
 * metadata caching, and retry logic.
 *
 * HOW IT CONNECTS TO WHAT WE BUILT:
 *
 * Stage 5 broker exposed these request types via TCP:
 *   PRODUCE, FETCH, METADATA, CREATE_TOPIC
 *
 * This client speaks those exact same bytes over the wire.
 * It's the other half of the protocol contract.
 *
 * BOOTSTRAP BROKER:
 *
 * The client only needs ONE broker's address to start.
 * That broker tells it about all the other brokers + who is leader for what.
 * This initial broker is called the "bootstrap broker".
 *
 * Real Kafka recommends pointing at 2-3 bootstrap brokers for redundancy.
 * We keep it simple with one.
 *
 * PACKAGE STRUCTURE:
 *   com.simplekafka.broker  ← server-side (Stages 1–5)
 *   com.simplekafka.client  ← client-side (Stage 6, this file)
 */
public class SimpleKafkaClient {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaClient.class.getName());

    // ─────────────────────────────────────────────
    // INNER CLASSES
    // ─────────────────────────────────────────────

    /**
     * PartitionMetadata holds what the client knows about ONE partition.
     *
     * When a client wants to send a message to "orders" partition 0,
     * it needs to know: which broker is the leader? What's its address?
     *
     * This is that answer, cached locally so we don't ask the broker every time.
     */
    public static class PartitionMetadata {
        public final int partitionId;
        public final int leaderId;
        public final String leaderHost;
        public final int leaderPort;

        public PartitionMetadata(int partitionId, int leaderId,
                                  String leaderHost, int leaderPort) {
            this.partitionId = partitionId;
            this.leaderId    = leaderId;
            this.leaderHost  = leaderHost;
            this.leaderPort  = leaderPort;
        }

        @Override
        public String toString() {
            return "Partition " + partitionId + " → broker " + leaderId
                    + " @ " + leaderHost + ":" + leaderPort;
        }
    }

    /**
     * TopicMetadata holds what the client knows about ONE topic:
     * a map of partitionId → PartitionMetadata.
     *
     * e.g. topic "orders" has:
     *   partition 0 → broker 1 @ localhost:9092
     *   partition 1 → broker 2 @ localhost:9093
     */
    public static class TopicMetadata {
        public final String topicName;
        public final Map<Integer, PartitionMetadata> partitions;

        public TopicMetadata(String topicName) {
            this.topicName  = topicName;
            this.partitions = new ConcurrentHashMap<>();
        }

        public void addPartition(PartitionMetadata pm) {
            partitions.put(pm.partitionId, pm);
        }

        public PartitionMetadata getPartition(int partitionId) {
            return partitions.get(partitionId);
        }

        public int partitionCount() {
            return partitions.size();
        }
    }

    // ─────────────────────────────────────────────
    // FIELDS
    // ─────────────────────────────────────────────

    /** Address of the ONE broker we contact first to discover the cluster */
    private final String bootstrapHost;
    private final int bootstrapPort;

    /**
     * Local cache of topic → partition → leader info.
     * We fill this on initialize() and refresh it whenever a request fails
     * (because the leader may have changed).
     *
     * ConcurrentHashMap because producer and consumer threads may access
     * this simultaneously.
     */
    private final Map<String, TopicMetadata> topicMetadataCache;

    // ─────────────────────────────────────────────
    // CONSTRUCTOR
    // ─────────────────────────────────────────────

    /**
     * @param bootstrapHost  hostname of ANY broker in the cluster
     * @param bootstrapPort  port of that broker
     *
     * The client doesn't connect yet — that happens in initialize().
     */
    public SimpleKafkaClient(String bootstrapHost, int bootstrapPort) {
        this.bootstrapHost      = bootstrapHost;
        this.bootstrapPort      = bootstrapPort;
        this.topicMetadataCache = new ConcurrentHashMap<>();
    }

    // ─────────────────────────────────────────────
    // LIFECYCLE
    // ─────────────────────────────────────────────

    /**
     * Call this once before using the client.
     * Connects to the bootstrap broker and loads initial metadata.
     *
     * In real Kafka this also starts background threads for batching,
     * heartbeating, etc. We keep it simple.
     */
    public void initialize() throws IOException {
        LOGGER.info("Initializing client → " + bootstrapHost + ":" + bootstrapPort);
        // Pre-warm: nothing to fetch yet since we don't know any topic names.
        // Metadata is fetched lazily on first send/fetch for a given topic,
        // or explicitly via refreshMetadata(topicName).
    }

    // ─────────────────────────────────────────────
    // METADATA
    // ─────────────────────────────────────────────

    /**
     * Asks the broker: "who is the leader for topic X, partition Y?"
     * and caches the answer.
     *
     * WHEN IS THIS CALLED?
     *   - Before the first send/fetch for a topic (lazy init)
     *   - When a send fails with "Not leader" (stale cache)
     *
     * METADATA REQUEST FORMAT (matches Stage 5 handleMetadataRequest):
     * ┌──────────┬───────────┬────────────────────┬──────────────┐
     * │ 1 byte   │ 2 bytes   │ N bytes            │ 4 bytes      │
     * │ METADATA │ topic len │ topic name (UTF-8) │ partition id │
     * └──────────┴───────────┴────────────────────┴──────────────┘
     *
     * METADATA RESPONSE FORMAT (matches Stage 5 handleMetadataRequest):
     * ┌──────────────────┬──────────┬──────────────────┬────────────────────┬──────────┐
     * │ 1 byte           │ 4 bytes  │ 2 bytes          │ N bytes            │ 4 bytes  │
     * │ METADATA_RESPONSE│ leaderId │ host name length │ leader host (UTF-8)│ port     │
     * └──────────────────┴──────────┴──────────────────┴────────────────────┴──────────┘
     *
     * @param topicName   the topic we want metadata for
     * @param partitionId the specific partition
     */
    public PartitionMetadata fetchMetadata(String topicName, int partitionId)
            throws IOException {

        byte[] topicBytes = topicName.getBytes("UTF-8");

        // Build the request
        ByteBuffer request = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4);
        request.put(Protocol.METADATA);
        request.putShort((short) topicBytes.length);
        request.put(topicBytes);
        request.putInt(partitionId);
        request.flip();

        // Send to bootstrap broker and read response
        try (SocketChannel channel = openConnection(bootstrapHost, bootstrapPort)) {
            channel.write(request);

            // Read response — allocate enough for header + host + port
            ByteBuffer response = ByteBuffer.allocate(256);
            readFully(channel, response, 1); // read type byte first

            byte responseType = response.get(0);
            if (responseType == Protocol.ERROR_RESPONSE) {
                String error = readErrorMessage(channel);
                throw new IOException("Metadata error: " + error);
            }

            // Read rest of response: leaderId (4) + hostLen (2) + host + port (4)
            response.clear();
            readFully(channel, response, 4 + 2);
            response.flip();

            int leaderId  = response.getInt();
            short hostLen = response.getShort();

            ByteBuffer hostBuf = ByteBuffer.allocate(hostLen);
            readFully(channel, hostBuf, hostLen);
            String leaderHost = new String(hostBuf.array(), "UTF-8");

            ByteBuffer portBuf = ByteBuffer.allocate(4);
            readFully(channel, portBuf, 4);
            portBuf.flip();
            int leaderPort = portBuf.getInt();

            PartitionMetadata pm = new PartitionMetadata(
                    partitionId, leaderId, leaderHost, leaderPort);

            // Cache it
            topicMetadataCache
                    .computeIfAbsent(topicName, TopicMetadata::new)
                    .addPartition(pm);

            LOGGER.info("Fetched metadata: " + pm);
            return pm;
        }
    }

    /**
     * Convenience: get cached metadata, or fetch it if we don't have it yet.
     * This is called internally before every send/fetch.
     */
    private PartitionMetadata getOrFetchMetadata(String topicName, int partitionId)
            throws IOException {

        TopicMetadata topicMeta = topicMetadataCache.get(topicName);
        if (topicMeta != null) {
            PartitionMetadata pm = topicMeta.getPartition(partitionId);
            if (pm != null) return pm;
        }
        return fetchMetadata(topicName, partitionId);
    }

    // ─────────────────────────────────────────────
    // PRODUCE (SEND)
    // ─────────────────────────────────────────────

    /**
     * Sends a message to a specific topic and partition.
     *
     * Steps:
     * 1. Look up (or fetch) which broker is the leader for this partition
     * 2. Connect directly to that leader broker
     * 3. Send the PRODUCE request
     * 4. Read back the assigned offset
     *
     * If the broker says "I'm not the leader", we refresh metadata and retry once.
     * (The leader may have changed due to a rebalance.)
     *
     * PRODUCE REQUEST FORMAT (matches Stage 5 handleProduceRequest):
     * ┌─────────┬───────────┬────────────────────┬──────────────┬────────────────┐
     * │ 1 byte  │ 2 bytes   │ N bytes            │ 4 bytes      │ remaining bytes│
     * │ PRODUCE │ topic len │ topic name (UTF-8) │ partition id │ message payload│
     * └─────────┴───────────┴────────────────────┴──────────────┴────────────────┘
     *
     * PRODUCE RESPONSE FORMAT:
     * ┌──────────────────┬──────────┐
     * │ 1 byte           │ 8 bytes  │
     * │ PRODUCE_RESPONSE │ offset   │
     * └──────────────────┴──────────┘
     *
     * @param topicName   name of the topic
     * @param partitionId which partition to write to
     * @param message     raw bytes to store
     * @return            the offset assigned to this message by the broker
     */
    public long send(String topicName, int partitionId, byte[] message)
            throws IOException {

        return sendWithRetry(topicName, partitionId, message, false);
    }

    /**
     * Internal send with one retry on "not leader" errors.
     *
     * @param isRetry  true if this call is already a retry — prevents infinite loops
     */
    private long sendWithRetry(String topicName, int partitionId,
                                byte[] message, boolean isRetry)
            throws IOException {

        PartitionMetadata meta = getOrFetchMetadata(topicName, partitionId);
        byte[] topicBytes = topicName.getBytes("UTF-8");

        ByteBuffer request = ByteBuffer.allocate(
                1 + 2 + topicBytes.length + 4 + message.length);
        request.put(Protocol.PRODUCE);
        request.putShort((short) topicBytes.length);
        request.put(topicBytes);
        request.putInt(partitionId);
        request.put(message);
        request.flip();

        try (SocketChannel channel = openConnection(meta.leaderHost, meta.leaderPort)) {
            channel.write(request);

            // Read the 1-byte type first
            ByteBuffer typeBuf = ByteBuffer.allocate(1);
            readFully(channel, typeBuf, 1);
            typeBuf.flip();
            byte responseType = typeBuf.get();

            if (responseType == Protocol.ERROR_RESPONSE) {
                String error = readErrorMessage(channel);

                // If the error says "Not leader", our cached metadata is stale
                if (!isRetry && error.contains("Not leader")) {
                    LOGGER.info("Stale leader — refreshing metadata and retrying...");

                    // Invalidate cache for this topic/partition
                    invalidateMetadata(topicName, partitionId);

                    // Retry once with fresh metadata
                    return sendWithRetry(topicName, partitionId, message, true);
                }

                throw new IOException("Produce failed: " + error);
            }

            if (responseType != Protocol.PRODUCE_RESPONSE) {
                throw new IOException("Unexpected response type: " + responseType);
            }

            // Read the 8-byte offset
            ByteBuffer offsetBuf = ByteBuffer.allocate(8);
            readFully(channel, offsetBuf, 8);
            offsetBuf.flip();
            long offset = offsetBuf.getLong();

            LOGGER.info("Sent to " + topicName + "-" + partitionId + " @ offset " + offset);
            return offset;
        }
    }

    // ─────────────────────────────────────────────
    // FETCH (CONSUME)
    // ─────────────────────────────────────────────

    /**
     * Fetches messages from a topic partition starting at a given offset.
     *
     * This is what a consumer calls. The consumer manages its own offset —
     * it keeps track of what it has already processed and passes the next
     * offset on each call. This is why Kafka consumers can replay messages
     * just by resetting their offset.
     *
     * FETCH REQUEST FORMAT (matches Stage 5 handleFetchRequest):
     * ┌────────┬───────────┬────────────────────┬──────────────┬──────────────┬──────────┐
     * │ 1 byte │ 2 bytes   │ N bytes            │ 4 bytes      │ 8 bytes      │ 4 bytes  │
     * │ FETCH  │ topic len │ topic name (UTF-8) │ partition id │ start offset │ maxBytes │
     * └────────┴───────────┴────────────────────┴──────────────┴──────────────┴──────────┘
     *
     * FETCH RESPONSE FORMAT:
     * ┌──────────────┬────────────┬─────────────────────────────────────────────────┐
     * │ 1 byte       │ 4 bytes    │ per message:                                    │
     * │ FETCH_RESPONSE│ msg count │ 8 bytes offset + 4 bytes length + N bytes data  │
     * └──────────────┴────────────┴─────────────────────────────────────────────────┘
     *
     * @param topicName   topic to read from
     * @param partitionId which partition
     * @param offset      start reading from this offset (inclusive)
     * @param maxBytes    stop once this many bytes have been returned
     * @return            list of raw message byte arrays
     */
    public List<byte[]> fetch(String topicName, int partitionId,
                               long offset, int maxBytes)
            throws IOException {

        PartitionMetadata meta = getOrFetchMetadata(topicName, partitionId);
        byte[] topicBytes = topicName.getBytes("UTF-8");

        ByteBuffer request = ByteBuffer.allocate(
                1 + 2 + topicBytes.length + 4 + 8 + 4);
        request.put(Protocol.FETCH);
        request.putShort((short) topicBytes.length);
        request.put(topicBytes);
        request.putInt(partitionId);
        request.putLong(offset);
        request.putInt(maxBytes);
        request.flip();

        try (SocketChannel channel = openConnection(meta.leaderHost, meta.leaderPort)) {
            channel.write(request);

            // Read type byte
            ByteBuffer typeBuf = ByteBuffer.allocate(1);
            readFully(channel, typeBuf, 1);
            typeBuf.flip();
            byte responseType = typeBuf.get();

            if (responseType == Protocol.ERROR_RESPONSE) {
                String error = readErrorMessage(channel);
                throw new IOException("Fetch failed: " + error);
            }

            if (responseType != Protocol.FETCH_RESPONSE) {
                throw new IOException("Unexpected response type: " + responseType);
            }

            // Read message count (4 bytes)
            ByteBuffer countBuf = ByteBuffer.allocate(4);
            readFully(channel, countBuf, 4);
            countBuf.flip();
            int messageCount = countBuf.getInt();

            List<byte[]> messages = new ArrayList<>(messageCount);

            // Read each message: 8 bytes offset + 4 bytes length + N bytes data
            for (int i = 0; i < messageCount; i++) {
                // Read offset (we don't use it here but it's in the protocol)
                ByteBuffer offsetBuf = ByteBuffer.allocate(8);
                readFully(channel, offsetBuf, 8);

                // Read message length
                ByteBuffer lenBuf = ByteBuffer.allocate(4);
                readFully(channel, lenBuf, 4);
                lenBuf.flip();
                int msgLen = lenBuf.getInt();

                // Read message data
                ByteBuffer msgBuf = ByteBuffer.allocate(msgLen);
                readFully(channel, msgBuf, msgLen);
                messages.add(msgBuf.array());
            }

            LOGGER.info("Fetched " + messages.size() + " messages from "
                    + topicName + "-" + partitionId + " @ offset " + offset);
            return messages;
        }
    }

    // ─────────────────────────────────────────────
    // TOPIC CREATION
    // ─────────────────────────────────────────────

    /**
     * Asks the broker to create a new topic.
     *
     * In real Kafka, topic creation is usually done via CLI tools or AdminClient.
     * Here we expose it directly on the client for simplicity.
     *
     * Only the controller broker will actually act on this.
     * If we hit a non-controller, it will log a warning but still return success
     * (the broker handles this gracefully in our Stage 5 implementation).
     *
     * CREATE_TOPIC REQUEST FORMAT:
     * ┌──────────────┬───────────┬────────────────────┬───────────────┬──────────────────┐
     * │ 1 byte       │ 2 bytes   │ N bytes            │ 4 bytes       │ 2 bytes          │
     * │ CREATE_TOPIC │ topic len │ topic name (UTF-8) │ numPartitions │ replicationFactor│
     * └──────────────┴───────────┴────────────────────┴───────────────┴──────────────────┘
     */
    public void createTopic(String topicName, int numPartitions,
                             short replicationFactor) throws IOException {

        byte[] topicBytes = topicName.getBytes("UTF-8");

        ByteBuffer request = ByteBuffer.allocate(
                1 + 2 + topicBytes.length + 4 + 2);
        request.put(Protocol.CREATE_TOPIC);
        request.putShort((short) topicBytes.length);
        request.put(topicBytes);
        request.putInt(numPartitions);
        request.putShort(replicationFactor);
        request.flip();

        try (SocketChannel channel = openConnection(bootstrapHost, bootstrapPort)) {
            channel.write(request);

            ByteBuffer ack = ByteBuffer.allocate(1);
            readFully(channel, ack, 1);
            ack.flip();

            if (ack.get() != 0x01) {
                throw new IOException("Topic creation failed — unexpected ACK");
            }

            LOGGER.info("Topic created: " + topicName
                    + " (" + numPartitions + " partitions, RF=" + replicationFactor + ")");
        }
    }

    // ─────────────────────────────────────────────
    // PRIVATE HELPERS
    // ─────────────────────────────────────────────

    /**
     * Opens a blocking TCP connection to a broker.
     *
     * We use configureBlocking(true) here (opposite of the server side).
     * The client doesn't need non-blocking I/O — it sends a request and
     * waits for a response, which is inherently sequential.
     *
     * try-with-resources in callers will auto-close the channel.
     */
    private SocketChannel openConnection(String host, int port) throws IOException {
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(true);
        channel.connect(new InetSocketAddress(host, port));
        return channel;
    }

    /**
     * Reads EXACTLY 'length' bytes from the channel into the buffer.
     *
     * WHY THIS EXISTS:
     * NIO's channel.read() is not guaranteed to fill the buffer in one call —
     * it may return fewer bytes if the data hasn't arrived yet over TCP.
     * We loop until we have exactly what we need.
     *
     * This is called "reading fully" and is a common pattern when working
     * with binary protocols over TCP.
     */
    private void readFully(SocketChannel channel, ByteBuffer buffer, int length)
            throws IOException {

        buffer.clear();
        buffer.limit(length);

        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) {
                throw new IOException("Connection closed before response was fully received");
            }
        }

        buffer.flip();
    }

    /**
     * Reads a length-prefixed error message from the channel.
     *
     * ERROR_RESPONSE format (after the 0x20 type byte):
     * ┌──────────┬────────────────────┐
     * │ 4 bytes  │ N bytes            │
     * │ length N │ UTF-8 error string │
     * └──────────┴────────────────────┘
     */
    private String readErrorMessage(SocketChannel channel) throws IOException {
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        readFully(channel, lenBuf, 4);
        lenBuf.flip();
        int len = lenBuf.getInt();

        ByteBuffer msgBuf = ByteBuffer.allocate(len);
        readFully(channel, msgBuf, len);
        return new String(msgBuf.array(), "UTF-8");
    }

    /**
     * Removes a cached partition metadata entry so the next request
     * triggers a fresh metadata fetch.
     * Called when we get a "Not leader" error.
     */
    private void invalidateMetadata(String topicName, int partitionId) {
        TopicMetadata topicMeta = topicMetadataCache.get(topicName);
        if (topicMeta != null) {
            topicMeta.partitions.remove(partitionId);
        }
    }

    // ─────────────────────────────────────────────
    // DEMO MAIN
    // ─────────────────────────────────────────────

    /**
     * Quick smoke test — assumes a broker is already running on localhost:9092.
     *
     * Run SimpleKafkaBroker.main() first, then run this.
     */
    public static void main(String[] args) throws Exception {
        SimpleKafkaClient client = new SimpleKafkaClient("localhost", 9092);
        client.initialize();

        // Create a topic
        client.createTopic("orders", 1, (short) 1);

        // Produce 3 messages
        for (int i = 0; i < 3; i++) {
            String msg = "order-" + i;
            long offset = client.send("orders", 0, msg.getBytes());
            System.out.println("Sent: '" + msg + "' → offset " + offset);
        }

        // Consume from the beginning
        List<byte[]> messages = client.fetch("orders", 0, 0L, 1024 * 1024);
        System.out.println("\nConsumed " + messages.size() + " messages:");
        for (byte[] m : messages) {
            System.out.println("  → " + new String(m));
        }
    }
}