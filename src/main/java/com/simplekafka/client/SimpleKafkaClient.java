package com.simplekafka.client;

import com.simplekafka.broker.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class SimpleKafkaClient {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaClient.class.getName());

    // ─────────────────────────────────────────────
    // INNER CLASSES
    // ─────────────────────────────────────────────

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
            return "Partition " + partitionId + " -> broker " + leaderId
                    + " @ " + leaderHost + ":" + leaderPort;
        }
    }

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

    private final String bootstrapHost;
    private final int bootstrapPort;
    private final Map<String, TopicMetadata> topicMetadataCache;

    // ─────────────────────────────────────────────
    // CONSTRUCTOR
    // ─────────────────────────────────────────────

    public SimpleKafkaClient(String bootstrapHost, int bootstrapPort) {
        this.bootstrapHost      = bootstrapHost;
        this.bootstrapPort      = bootstrapPort;
        this.topicMetadataCache = new ConcurrentHashMap<>();
    }

    // ─────────────────────────────────────────────
    // LIFECYCLE
    // ─────────────────────────────────────────────

    public void initialize() throws IOException {
        LOGGER.info("Initializing client -> " + bootstrapHost + ":" + bootstrapPort);
    }

    // ─────────────────────────────────────────────
    // METADATA
    // ─────────────────────────────────────────────

    /**
     * Asks the broker who is the leader for topic/partition and caches the answer.
     *
     * REQUEST FORMAT:
     *   1 byte  METADATA
     *   2 bytes topic name length
     *   N bytes topic name (UTF-8)
     *   4 bytes partition id
     *
     * RESPONSE FORMAT:
     *   1 byte  METADATA_RESPONSE
     *   4 bytes leaderId
     *   2 bytes leader host length
     *   N bytes leader host (UTF-8)
     *   4 bytes leader port
     */
    public PartitionMetadata fetchMetadata(String topicName, int partitionId)
            throws IOException {

        byte[] topicBytes = topicName.getBytes("UTF-8");

        ByteBuffer request = ByteBuffer.allocate(1 + 2 + topicBytes.length + 4);
        request.put(Protocol.METADATA);
        request.putShort((short) topicBytes.length);
        request.put(topicBytes);
        request.putInt(partitionId);
        request.flip();

        try (SocketChannel channel = openConnection(bootstrapHost, bootstrapPort)) {
            channel.write(request);

            // Read type byte
            ByteBuffer typeBuf = ByteBuffer.allocate(1);
            readFully(channel, typeBuf, 1);
            byte responseType = typeBuf.get();

            if (responseType == Protocol.ERROR_RESPONSE) {
                throw new IOException("Metadata error: " + readErrorMessage(channel));
            }

            // Read leaderId (4) + hostLen (2)
            ByteBuffer headerBuf = ByteBuffer.allocate(6);
            readFully(channel, headerBuf, 6);
            int leaderId  = headerBuf.getInt();
            short hostLen = headerBuf.getShort();

            // Read host string
            ByteBuffer hostBuf = ByteBuffer.allocate(hostLen);
            readFully(channel, hostBuf, hostLen);
            String leaderHost = new String(hostBuf.array(), "UTF-8");

            // Read port
            ByteBuffer portBuf = ByteBuffer.allocate(4);
            readFully(channel, portBuf, 4);
            int leaderPort = portBuf.getInt();

            PartitionMetadata pm = new PartitionMetadata(
                    partitionId, leaderId, leaderHost, leaderPort);

            // Cache it under the topic
            topicMetadataCache
                    .computeIfAbsent(topicName, TopicMetadata::new)
                    .addPartition(pm);

            LOGGER.info("Fetched metadata: " + pm);
            return pm;
        }
    }

    /**
     * Returns cached metadata or fetches from broker if not yet cached.
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

    /**
     * Exposes the metadata cache so SimpleKafkaProducer can read partition counts.
     * Returns an unmodifiable view to prevent accidental corruption.
     *
     * THIS IS THE METHOD SIMPLEKAFKAPRODUCER CALLS — it must live here in
     * SimpleKafkaClient, not in SimpleKafkaProducer.
     */
    public Map<String, TopicMetadata> getTopicMetadataCache() {
        return Collections.unmodifiableMap(topicMetadataCache);
    }

    // ─────────────────────────────────────────────
    // PRODUCE
    // ─────────────────────────────────────────────

    /**
     * Sends a message to a specific topic partition.
     * Retries once with fresh metadata if the broker says "Not leader".
     *
     * REQUEST FORMAT:
     *   1 byte  PRODUCE
     *   2 bytes topic name length
     *   N bytes topic name (UTF-8)
     *   4 bytes partition id
     *   remaining: message payload
     *
     * RESPONSE FORMAT:
     *   1 byte  PRODUCE_RESPONSE
     *   8 bytes assigned offset
     */
    public long send(String topicName, int partitionId, byte[] message)
            throws IOException {
        return sendWithRetry(topicName, partitionId, message, false);
    }

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

            ByteBuffer typeBuf = ByteBuffer.allocate(1);
            readFully(channel, typeBuf, 1);
            byte responseType = typeBuf.get();

            if (responseType == Protocol.ERROR_RESPONSE) {
                String error = readErrorMessage(channel);
                if (!isRetry && error.contains("Not leader")) {
                    LOGGER.info("Stale leader — refreshing metadata and retrying...");
                    invalidateMetadata(topicName, partitionId);
                    return sendWithRetry(topicName, partitionId, message, true);
                }
                throw new IOException("Produce failed: " + error);
            }

            if (responseType != Protocol.PRODUCE_RESPONSE) {
                throw new IOException("Unexpected response type: " + responseType);
            }

            ByteBuffer offsetBuf = ByteBuffer.allocate(8);
            readFully(channel, offsetBuf, 8);
            long offset = offsetBuf.getLong();

            LOGGER.info("Sent to " + topicName + "-" + partitionId + " @ offset " + offset);
            return offset;
        }
    }

    // ─────────────────────────────────────────────
    // FETCH
    // ─────────────────────────────────────────────

    /**
     * Fetches messages from a topic partition starting at a given offset.
     *
     * REQUEST FORMAT:
     *   1 byte  FETCH
     *   2 bytes topic name length
     *   N bytes topic name (UTF-8)
     *   4 bytes partition id
     *   8 bytes start offset
     *   4 bytes max bytes
     *
     * RESPONSE FORMAT:
     *   1 byte  FETCH_RESPONSE
     *   4 bytes message count
     *   per message: 8 bytes offset + 4 bytes length + N bytes data
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

            ByteBuffer typeBuf = ByteBuffer.allocate(1);
            readFully(channel, typeBuf, 1);
            byte responseType = typeBuf.get();

            if (responseType == Protocol.ERROR_RESPONSE) {
                throw new IOException("Fetch failed: " + readErrorMessage(channel));
            }
            if (responseType != Protocol.FETCH_RESPONSE) {
                throw new IOException("Unexpected response type: " + responseType);
            }

            ByteBuffer countBuf = ByteBuffer.allocate(4);
            readFully(channel, countBuf, 4);
            int messageCount = countBuf.getInt();

            List<byte[]> messages = new ArrayList<>(messageCount);
            for (int i = 0; i < messageCount; i++) {
                readFully(channel, ByteBuffer.allocate(8), 8); // skip offset field

                ByteBuffer lenBuf = ByteBuffer.allocate(4);
                readFully(channel, lenBuf, 4);
                int msgLen = lenBuf.getInt();

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
     * Asks the broker to create a topic.
     *
     * REQUEST FORMAT:
     *   1 byte  CREATE_TOPIC
     *   2 bytes topic name length
     *   N bytes topic name (UTF-8)
     *   4 bytes numPartitions
     *   2 bytes replicationFactor
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

    private SocketChannel openConnection(String host, int port) throws IOException {
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(true);
        channel.connect(new InetSocketAddress(host, port));
        return channel;
    }

    /**
     * Reads EXACTLY 'length' bytes from channel into buffer.
     * NIO channel.read() may return fewer bytes than requested — we loop until full.
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
     * Reads a length-prefixed UTF-8 error string from the channel.
     * Called after reading an ERROR_RESPONSE (0x20) type byte.
     *
     * FORMAT: 4 bytes length + N bytes UTF-8 string
     */
    private String readErrorMessage(SocketChannel channel) throws IOException {
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        readFully(channel, lenBuf, 4);
        int len = lenBuf.getInt();

        ByteBuffer msgBuf = ByteBuffer.allocate(len);
        readFully(channel, msgBuf, len);
        return new String(msgBuf.array(), "UTF-8");
    }

    private void invalidateMetadata(String topicName, int partitionId) {
        TopicMetadata topicMeta = topicMetadataCache.get(topicName);
        if (topicMeta != null) {
            topicMeta.partitions.remove(partitionId);
        }
    }

    // ─────────────────────────────────────────────
    // MAIN
    // ─────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        SimpleKafkaClient client = new SimpleKafkaClient("localhost", 9092);
        client.initialize();

        client.createTopic("orders", 1, (short) 1);

        for (int i = 0; i < 3; i++) {
            String msg = "order-" + i;
            long offset = client.send("orders", 0, msg.getBytes());
            System.out.println("Sent: '" + msg + "' -> offset " + offset);
        }

        List<byte[]> messages = client.fetch("orders", 0, 0L, 1024 * 1024);
        System.out.println("\nConsumed " + messages.size() + " messages:");
        for (byte[] m : messages) {
            System.out.println("  -> " + new String(m));
        }
    }
}