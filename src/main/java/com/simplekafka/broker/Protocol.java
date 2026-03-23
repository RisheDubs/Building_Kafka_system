package com.simplekafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * WHAT IS A WIRE PROTOCOL?
 *
 * When a producer sends a message to a broker over TCP, it's just a stream of bytes.
 * The protocol is the contract both sides agree on:
 *   "The first byte tells you WHAT kind of message this is.
 *    The rest of the bytes are the payload, in a specific order."
 *
 * Think of it like an envelope:
 *   - The first byte = the label on the envelope (PRODUCE, FETCH, etc.)
 *   - The remaining bytes = the letter inside
 *
 * We use bytes (not JSON/HTTP) because it's much faster and smaller on the wire.
 * Real Kafka uses a very similar binary protocol.
 *
 * STAGE 5 ADDITIONS:
 * We've added REPLICATE, TOPIC_NOTIFICATION, and helper methods for sending responses.
 * These are needed now that brokers talk to each other, not just to clients.
 */
public class Protocol {

    // ─────────────────────────────────────────────
    // REQUEST TYPES (client → broker)
    // ─────────────────────────────────────────────

    /** Producer sends a message to a topic/partition */
    public static final byte PRODUCE           = 0x01;

    /** Consumer fetches messages from a topic/partition at a given offset */
    public static final byte FETCH             = 0x02;

    /** Client asks: "which broker is leader for topic X, partition Y?" */
    public static final byte METADATA          = 0x03;

    /** Controller creates a new topic with N partitions */
    public static final byte CREATE_TOPIC      = 0x04;

    /**
     * NEW IN STAGE 5:
     * Leader broker sends a copy of a message to a follower broker.
     * This is broker-to-broker communication, not client-to-broker.
     */
    public static final byte REPLICATE         = 0x05;

    /**
     * NEW IN STAGE 5:
     * Controller notifies all brokers: "a new topic was created, load its metadata."
     * Brokers can't produce/consume a topic they don't know about locally.
     */
    public static final byte TOPIC_NOTIFICATION = 0x06;

    // ─────────────────────────────────────────────
    // RESPONSE TYPES (broker → client)
    // ─────────────────────────────────────────────

    public static final byte PRODUCE_RESPONSE  = 0x11;
    public static final byte FETCH_RESPONSE    = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte ERROR_RESPONSE    = 0x20;

    // ─────────────────────────────────────────────
    // HELPER: Send an error back to caller
    // ─────────────────────────────────────────────

    /**
     * Sends a standardised error response over a SocketChannel.
     *
     * FORMAT:
     * ┌──────────┬───────────────────────────────────────┐
     * │ 1 byte   │ 4 bytes + N bytes                     │
     * │ ERROR(0x20) │ length-prefixed UTF-8 error string  │
     * └──────────┴───────────────────────────────────────┘
     *
     * We use this in handleProduceRequest, handleFetchRequest, etc.
     * whenever something goes wrong — so the client doesn't just hang waiting.
     */
    public static void sendErrorResponse(SocketChannel channel, String errorMessage)
            throws IOException {

        byte[] errorBytes = errorMessage.getBytes("UTF-8");

        // 1 (type) + 4 (length) + N (message)
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + errorBytes.length);
        buffer.put(ERROR_RESPONSE);
        buffer.putInt(errorBytes.length);
        buffer.put(errorBytes);
        buffer.flip();

        channel.write(buffer);
    }
}