package com.simplekafka.broker;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * WHAT IS A PARTITION?
 *
 * In Kafka, a topic is split into partitions. Think of a topic like a database table,
 * and partitions are shards of that table spread across brokers.
 *
 * Each partition is an ORDERED, IMMUTABLE sequence of messages — like a log file.
 * Messages are always appended to the end. They are never updated or deleted mid-log.
 *
 * This class manages ONE partition's storage on disk.
 *
 * ON-DISK LAYOUT:
 * --------------------------------------------------
 * data/topic-0/                   ← baseDir
 *   00000000000000000000.log       ← segment 1: raw messages
 *   00000000000000000000.index     ← segment 1: offset→position map
 *   00000000000000000500.log       ← segment 2: starts at offset 500
 *   00000000000000000500.index
 * --------------------------------------------------
 *
 * WHY SEGMENTS?
 * If you stored everything in one file, it would grow forever.
 * Segments let Kafka delete old data cleanly (just delete old segment files).
 * They also make startup faster — you don't scan one giant file.
 */
public class Partition {

    // ─────────────────────────────────────────────
    // CONSTANTS
    // ─────────────────────────────────────────────

    /** Each segment rolls over after 1MB. Real Kafka default is 1GB. */
    private static final long MAX_SEGMENT_SIZE = 1024 * 1024; // 1 MB

    /** Each index entry = 8 bytes (offset) + 8 bytes (file position) = 16 bytes */
    private static final int INDEX_ENTRY_SIZE = 16;

    // ─────────────────────────────────────────────
    // FIELDS
    // ─────────────────────────────────────────────

    private final int id;               // e.g., partition 0, 1, 2
    private int leader;                 // which broker is leader for this partition
    private List<Integer> followers;    // which brokers hold replicas

    private final String baseDir;       // directory on disk for this partition's files

    /**
     * AtomicLong is a thread-safe counter.
     * Every message gets a unique, ever-increasing offset (0, 1, 2, 3...).
     * This tracks what the NEXT offset will be.
     */
    private final AtomicLong nextOffset;

    /**
     * ReadWriteLock allows MULTIPLE readers OR ONE writer at a time.
     * This is smarter than synchronizing everything — reads don't block each other.
     */
    private final ReadWriteLock lock;

    /** The currently active .log file we're appending to */
    private RandomAccessFile activeLogFile;

    /** FileChannel is Java NIO — faster than old InputStream/OutputStream for disk I/O */
    private FileChannel activeLogChannel;

    /** In-memory list of all segments. We load this from disk on startup. */
    private final List<SegmentInfo> segments;

    // ─────────────────────────────────────────────
    // INNER CLASS: SegmentInfo
    // ─────────────────────────────────────────────

    /**
     * Holds metadata about ONE segment.
     * A segment = a pair of files: .log (messages) and .index (offset map).
     *
     * baseOffset: the offset of the FIRST message in this segment.
     * The file name IS the baseOffset (zero-padded to 20 digits).
     */
    private static class SegmentInfo {
        final long baseOffset;
        final File logFile;
        final File indexFile;

        SegmentInfo(long baseOffset, File logFile, File indexFile) {
            this.baseOffset = baseOffset;
            this.logFile = logFile;
            this.indexFile = indexFile;
        }
    }

    // ─────────────────────────────────────────────
    // CONSTRUCTOR
    // ─────────────────────────────────────────────

    public Partition(int id, String baseDir) {
        this.id = id;
        this.baseDir = baseDir;
        this.followers = new ArrayList<>();
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();
    }

    // ─────────────────────────────────────────────
    // INITIALIZE
    // ─────────────────────────────────────────────

    /**
     * Called once when the broker starts up.
     *
     * WHY THIS EXISTS:
     * If the broker crashed and restarted, there are already files on disk.
     * We need to RECOVER state — find existing segments, figure out what the
     * last offset was, and pick up where we left off.
     *
     * If there's nothing on disk (first time), we create the first segment.
     */
    public void initialize() throws IOException {
        // Step 1: Make sure the directory exists
        File dir = new File(baseDir);
        if (!dir.exists()) {
            dir.mkdirs(); // creates full path if needed
        }

        // Step 2: Scan directory for existing .log files
        // Each .log file represents one segment.
        // File names look like: 00000000000000000000.log
        //                       ^--- this number IS the baseOffset
        File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));

        if (logFiles != null && logFiles.length > 0) {
            // Step 3: Parse each file name to get the baseOffset
            for (File logFile : logFiles) {
                String name = logFile.getName(); // e.g. "00000000000000000500.log"
                long baseOffset = Long.parseLong(name.replace(".log", ""));

                // The index file has the same base name, different extension
                File indexFile = new File(dir, String.format("%020d.index", baseOffset));

                segments.add(new SegmentInfo(baseOffset, logFile, indexFile));
            }

            // Step 4: Sort segments by baseOffset (oldest first)
            segments.sort(Comparator.comparingLong(s -> s.baseOffset));

            // Step 5: Figure out the next offset by reading the last segment
            // We count how many messages are in the last segment,
            // then: nextOffset = lastSegment.baseOffset + messageCount
            SegmentInfo lastSegment = segments.get(segments.size() - 1);
            long messageCount = countMessagesInSegment(lastSegment);
            nextOffset.set(lastSegment.baseOffset + messageCount);

        } else {
            // No existing files → fresh start, create segment 0
            createNewSegment(0);
        }

        // Step 6: Open the last segment for appending new messages
        openSegmentForAppend(segments.get(segments.size() - 1));

        System.out.println("Partition " + id + " initialized. Next offset: " + nextOffset.get());
    }

    // ─────────────────────────────────────────────
    // APPEND (WRITE PATH)
    // ─────────────────────────────────────────────

    /**
     * Writes a new message to this partition.
     *
     * MESSAGE FORMAT ON DISK:
     * ┌──────────────┬──────────────────────┐
     * │  4 bytes     │  N bytes             │
     * │  size = N    │  message payload     │
     * └──────────────┴──────────────────────┘
     *
     * Why a size prefix? So the reader knows how many bytes to read.
     * This is called "length-prefixed framing" — very common in binary protocols.
     *
     * @return the offset assigned to this message
     */
    public long append(byte[] message) throws IOException {
        // Acquire WRITE lock — no other thread can read or write while we're here
        lock.writeLock().lock();
        try {
            // Roll to a new segment if current one is too big
            if (activeLogFile.length() >= MAX_SEGMENT_SIZE) {
                createNewSegment(nextOffset.get());
                openSegmentForAppend(segments.get(segments.size() - 1));
            }

            // Record where in the file this message starts (for the index)
            long position = activeLogChannel.position();

            // Build a buffer: 4 bytes for size + N bytes for message
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length); // write size first
            buffer.put(message);           // then the actual message
            buffer.flip();                 // flip switches from write mode to read mode

            // Write to disk via FileChannel (faster than OutputStream)
            activeLogChannel.write(buffer);

            // force(true) = fsync — ensures the OS flushes data to physical disk
            // Without this, data could be lost on a crash. This is how Kafka guarantees durability.
            activeLogChannel.force(true);

            // Assign this message its offset, then increment for the next one
            long offset = nextOffset.getAndIncrement();

            // Update the index so we can find this message quickly later
            updateIndex(offset, position);

            return offset;

        } finally {
            // ALWAYS release the lock, even if an exception was thrown
            lock.writeLock().unlock();
        }
    }

    // ─────────────────────────────────────────────
    // READ PATH
    // ─────────────────────────────────────────────

    /**
     * Reads messages starting from a given offset, up to maxBytes total.
     *
     * WHY offset + maxBytes?
     * Consumers tell the broker: "give me messages starting at offset 42,
     * but don't send more than 1MB." This controls memory and network usage.
     *
     * @param startOffset  the first offset to read
     * @param maxBytes     stop reading after accumulating this many bytes
     */
    public List<byte[]> readMessages(long startOffset, int maxBytes) throws IOException {
        // Acquire READ lock — multiple threads can read simultaneously
        lock.readLock().lock();
        try {
            List<byte[]> messages = new ArrayList<>();
            int bytesRead = 0;

            // Step 1: Which segment contains this offset?
            SegmentInfo segment = findSegmentForOffset(startOffset);
            if (segment == null) return messages; // offset doesn't exist

            // Step 2: Where in the .log file is this offset?
            long filePosition = findPositionForOffset(segment, startOffset);

            // Step 3: Read messages from that file position
            // We might need to cross into the next segment if we run out
            int segmentIndex = segments.indexOf(segment);

            while (bytesRead < maxBytes && segmentIndex < segments.size()) {
                SegmentInfo currentSegment = segments.get(segmentIndex);

                try (RandomAccessFile raf = new RandomAccessFile(currentSegment.logFile, "r")) {
                    raf.seek(filePosition); // jump directly to the right position

                    while (bytesRead < maxBytes && raf.getFilePointer() < raf.length()) {
                        // Read the 4-byte size prefix
                        int size = raf.readInt();
                        if (size <= 0) break;

                        // Read exactly 'size' bytes for the message
                        byte[] messageData = new byte[size];
                        raf.readFully(messageData);

                        messages.add(messageData);
                        bytesRead += size;
                    }
                }

                // Move to next segment, start from the beginning of its file
                segmentIndex++;
                filePosition = 0;
            }

            return messages;

        } finally {
            lock.readLock().unlock();
        }
    }

    // ─────────────────────────────────────────────
    // SEGMENT MANAGEMENT
    // ─────────────────────────────────────────────

    /**
     * Creates a brand new segment starting at baseOffset.
     *
     * WHY 20-DIGIT PADDING?
     * File names like "00000000000000000500.log" sort correctly alphabetically.
     * Without padding, "1000.log" would sort before "500.log" (lexicographic order).
     * With 20-digit padding, alphabetical order = chronological order.
     */
    private void createNewSegment(long baseOffset) throws IOException {
        File dir = new File(baseDir);
        String baseName = String.format("%020d", baseOffset); // e.g. "00000000000000000500"

        File logFile   = new File(dir, baseName + ".log");
        File indexFile = new File(dir, baseName + ".index");

        // Create the files on disk (empty)
        logFile.createNewFile();
        indexFile.createNewFile();

        segments.add(new SegmentInfo(baseOffset, logFile, indexFile));
        System.out.println("Created new segment at offset " + baseOffset);
    }

    /**
     * Opens a segment's .log file for appending.
     * Closes any previously open file handles first to avoid resource leaks.
     */
    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        // Close old handles if open
        if (activeLogChannel != null && activeLogChannel.isOpen()) {
            activeLogChannel.close();
        }
        if (activeLogFile != null) {
            activeLogFile.close();
        }

        // Open in "rw" mode (read-write)
        activeLogFile = new RandomAccessFile(segment.logFile, "rw");

        // Seek to END of file — we always append, never overwrite
        activeLogFile.seek(activeLogFile.length());

        // Wrap with FileChannel for NIO access
        activeLogChannel = activeLogFile.getChannel();
    }

    /**
     * Finds which segment contains a given offset.
     *
     * HOW IT WORKS:
     * Segments are sorted by baseOffset. A segment "owns" offsets from
     * its baseOffset up to (but not including) the next segment's baseOffset.
     *
     * Example: segments at [0, 500, 1000]
     *   offset 750 → segment starting at 500
     *   offset 1200 → segment starting at 1000 (last segment, no upper bound)
     */
    private SegmentInfo findSegmentForOffset(long offset) {
        if (segments.isEmpty()) return null;

        // Binary search: find the last segment whose baseOffset <= offset
        int lo = 0, hi = segments.size() - 1, result = -1;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (segments.get(mid).baseOffset <= offset) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        return result >= 0 ? segments.get(result) : null;
    }

    // ─────────────────────────────────────────────
    // INDEX MANAGEMENT
    // ─────────────────────────────────────────────

    /**
     * Appends an entry to the index file.
     *
     * INDEX FORMAT:
     * Each entry = 16 bytes
     * ┌────────────────┬──────────────────┐
     * │  8 bytes       │  8 bytes         │
     * │  offset (long) │  position (long) │
     * └────────────────┴──────────────────┘
     *
     * WHY AN INDEX?
     * Without it, to read offset 5000 you'd have to scan from offset 0,
     * reading through every message. That's O(n) — terrible for large logs.
     * The index lets you jump directly to the file position for any offset.
     */
    private void updateIndex(long offset, long position) throws IOException {
        SegmentInfo currentSegment = segments.get(segments.size() - 1);

        // Open index file in append mode
        try (RandomAccessFile indexRaf = new RandomAccessFile(currentSegment.indexFile, "rw")) {
            indexRaf.seek(indexRaf.length()); // always append

            FileChannel indexChannel = indexRaf.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            buffer.putLong(offset);    // 8 bytes: the message offset
            buffer.putLong(position);  // 8 bytes: where in the .log file
            buffer.flip();

            indexChannel.write(buffer);
            indexChannel.force(true); // flush to disk
        }
    }

    /**
     * Looks up the file position for a given offset using the index.
     *
     * SPARSE INDEX:
     * Kafka doesn't index every single message — that would make the index as big as the log.
     * Instead it indexes periodically (every N bytes). So we find the CLOSEST indexed offset
     * that is <= our target, then scan forward from there.
     *
     * In our simplified version we index every message, so lookup is exact.
     */
    private long findPositionForOffset(SegmentInfo segment, long offset) throws IOException {
        // The index stores offsets relative to the segment's baseOffset
        // e.g., segment starts at offset 500, message at offset 503
        //       → relative offset = 3 → index entry #3 → byte position 3 * 16 = 48
        long relativeOffset = offset - segment.baseOffset;

        try (RandomAccessFile indexRaf = new RandomAccessFile(segment.indexFile, "r")) {
            long indexSize = indexRaf.length();
            if (indexSize == 0) return 0; // empty index, start from beginning of log

            // Calculate where in the index file this entry should be
            long indexPosition = relativeOffset * INDEX_ENTRY_SIZE;

            // If out of range, start from 0 (will scan forward from file start)
            if (indexPosition >= indexSize) return 0;

            indexRaf.seek(indexPosition);

            long storedOffset   = indexRaf.readLong(); // should match `offset`
            long filePosition   = indexRaf.readLong(); // position in .log file

            return filePosition;
        }
    }

    // ─────────────────────────────────────────────
    // RECOVERY HELPER
    // ─────────────────────────────────────────────

    /**
     * Counts how many messages exist in a segment by scanning its .log file.
     * Used on startup to reconstruct nextOffset after a crash.
     *
     * We can't trust the index alone for this — it might not be fully flushed.
     * The .log file is the source of truth.
     */
    private long countMessagesInSegment(SegmentInfo segment) throws IOException {
        long count = 0;
        try (RandomAccessFile raf = new RandomAccessFile(segment.logFile, "r")) {
            while (raf.getFilePointer() < raf.length()) {
                int size = raf.readInt(); // read 4-byte size prefix
                if (size <= 0) break;
                raf.skipBytes(size);      // skip message body
                count++;
            }
        }
        return count;
    }

    // ─────────────────────────────────────────────
    // GETTERS / SETTERS
    // ─────────────────────────────────────────────

    public int getId() { return id; }

    public int getLeader() { return leader; }
    public void setLeader(int leader) { this.leader = leader; }

    public List<Integer> getFollowers() { return Collections.unmodifiableList(followers); }
    public void setFollowers(List<Integer> followers) { this.followers = new ArrayList<>(followers); }

    public long getNextOffset() { return nextOffset.get(); }
}