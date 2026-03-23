package com.simplekafka.broker;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WHAT CHANGED FROM STAGE 3?
 *
 * Stage 3 SimpleKafkaBroker:
 *   - Connected to ZooKeeper
 *   - Registered itself
 *   - Elected a controller
 *   - That's it. No networking. No messages. No topics.
 *
 * Stage 5 SimpleKafkaBroker:
 *   - All of the above PLUS:
 *   - Opens a real TCP server socket (NIO ServerSocketChannel)
 *   - Accepts client connections in a loop
 *   - Reads binary protocol messages (PRODUCE, FETCH, METADATA, etc.)
 *   - Routes to the right handler method
 *   - Manages topics and partitions (creates them, stores metadata in ZK)
 *   - Replicates messages from leader → followers
 *   - Rebalances partitions when cluster membership changes
 *
 * Think of this as the difference between a building's address (Stage 3)
 * and the actual building with rooms, staff, and services (Stage 5).
 *
 * ARCHITECTURE OVERVIEW:
 *
 *  Producer/Consumer
 *       │  TCP
 *       ▼
 *  ServerSocketChannel  ← acceptConnections() loop
 *       │
 *       ▼  (one thread per client from executor pool)
 *  handleClient()
 *       │
 *       ▼
 *  processClientMessage()  ← reads first byte = message type
 *       │
 *       ├─ PRODUCE         → handleProduceRequest()
 *       ├─ FETCH           → handleFetchRequest()
 *       ├─ METADATA        → handleMetadataRequest()
 *       ├─ CREATE_TOPIC    → handleCreateTopicRequest()
 *       ├─ REPLICATE       → handleReplicateRequest()  ← broker-to-broker
 *       └─ TOPIC_NOTIFICATION → handleTopicNotification()  ← broker-to-broker
 */
public class SimpleKafkaBroker {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());

    // Where to store partition data on disk: data/<brokerId>/
    private static final String DATA_DIR = "data";

    // ─────────────────────────────────────────────
    // FIELDS
    // ─────────────────────────────────────────────

    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;

    /**
     * topics: our in-memory registry of everything this broker knows about.
     * Key   = topic name (e.g., "orders")
     * Value = map of partitionId → Partition object
     *
     * ConcurrentHashMap because multiple threads (one per client) read/write this.
     */
    private final ConcurrentHashMap<String, Map<Integer, Partition>> topics;

    /**
     * clusterMetadata: what we know about other brokers in the cluster.
     * Key   = brokerId (as String, from ZooKeeper node names)
     * Value = BrokerInfo (host + port)
     *
     * Used when we need to forward requests to the leader, or replicate to followers.
     */
    private final ConcurrentHashMap<String, BrokerInfo> clusterMetadata;

    /** Thread pool: each accepted client connection gets its own thread */
    private final ExecutorService executor;

    /** The server socket — listens for incoming TCP connections on brokerPort */
    private final ServerSocketChannel serverChannel;

    /** Flipped to false when stop() is called — signals all loops to exit */
    private final AtomicBoolean isRunning;

    /**
     * isController: only ONE broker in the cluster is the controller.
     * The controller is responsible for:
     *   - Assigning partition leaders when a topic is created
     *   - Rebalancing when a broker joins or leaves
     *   - Being the only one who can create topics
     */
    private final AtomicBoolean isController;

    private ZooKeeperClient zkClient;

    // ─────────────────────────────────────────────
    // CONSTRUCTOR
    // ─────────────────────────────────────────────

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId    = brokerId;
        this.brokerHost  = host;
        this.brokerPort  = port;
        this.topics         = new ConcurrentHashMap<>();
        this.clusterMetadata = new ConcurrentHashMap<>();
        this.executor    = Executors.newFixedThreadPool(10);
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning   = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);

        // Create local data directory: data/1/, data/2/, etc.
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) dataDir.mkdirs();

        this.zkClient = new ZooKeeperClient("localhost", zkPort);
    }

    // ─────────────────────────────────────────────
    // START / STOP
    // ─────────────────────────────────────────────

    /**
     * Startup sequence — order matters here:
     * 1. Open network socket FIRST (so we can accept connections)
     * 2. Register with ZooKeeper (announce we exist)
     * 3. Elect controller (decide who's in charge)
     * 4. Load existing topic metadata (recover state from ZK after a restart)
     * 5. Start accepting client connections
     */
    public void start() throws Exception {
        // Step 1: Bind to our port
        // configureBlocking(false) = non-blocking mode; accept() returns null instead of blocking
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(brokerHost, brokerPort));
        isRunning.set(true);
        LOGGER.info("Broker " + brokerId + " listening on " + brokerHost + ":" + brokerPort);

        // Step 2: Register with ZooKeeper
        registerWithZookeeper();

        // Step 3: Try to become controller
        electController();

        // Step 4: Recover topic state from ZooKeeper (survives broker restarts)
        loadExistingTopics();

        // Step 5: Start the connection-accepting loop in a background thread
        executor.submit(this::acceptConnections);

        LOGGER.info("Broker " + brokerId + " started successfully");
    }

    /**
     * Graceful shutdown:
     * - Stop accepting new connections
     * - Close all partition files (flushes any buffered data)
     * - Shut down the thread pool
     * - Disconnect from ZooKeeper (this also auto-deletes our ephemeral nodes)
     */
    public void stop() {
        isRunning.set(false);
        try {
            serverChannel.close();

            // Close all partition log files
            for (Map<Integer, Partition> partitionMap : topics.values()) {
                // Partition doesn't have a close() yet — add one if you want clean shutdown
            }

            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);

            if (zkClient != null) zkClient.close();

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error during shutdown", e);
        }
        LOGGER.info("Broker " + brokerId + " stopped");
    }

    // ─────────────────────────────────────────────
    // ZOOKEEPER REGISTRATION
    // ─────────────────────────────────────────────

    /**
     * Announces this broker to the cluster via ZooKeeper.
     *
     * Creates an EPHEMERAL node at: /brokers/<brokerId>
     * Data stored: "host:port"   e.g. "localhost:9092"
     *
     * Ephemeral = when this broker's ZK session ends (crash/disconnect),
     * ZooKeeper automatically deletes this node — other brokers see us leave.
     *
     * Also sets up a watch so we get notified when any broker joins or leaves.
     */
    private void registerWithZookeeper() {
        try {
            zkClient.connect();

            // Store our address so other brokers can connect to us
            String brokerData = brokerHost + ":" + brokerPort;
            String brokerPath = "/brokers/" + brokerId;
            zkClient.createEphemeralNode(brokerPath, brokerData);

            // Add ourselves to local cluster metadata
            clusterMetadata.put(String.valueOf(brokerId),
                    new BrokerInfo(brokerId, brokerHost, brokerPort));

            // Watch /brokers for changes (other brokers joining/leaving)
            zkClient.watchChildren("/brokers", this::onBrokersChanged);

            LOGGER.info("Registered broker " + brokerId + " with ZooKeeper");

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
        }
    }

    /**
     * Called whenever the list of brokers in ZooKeeper changes.
     * (A broker joined or left the cluster.)
     *
     * What we do:
     * 1. Update our in-memory clusterMetadata map
     * 2. Remove brokers that are no longer listed
     * 3. If we're the controller: rebalance partitions (a leader may have gone down)
     * 4. If we're NOT the controller: try to become one (maybe the controller left)
     */
    private void onBrokersChanged(List<String> brokerIds) {
        LOGGER.info("Cluster changed. Active brokers: " + brokerIds);

        // Add any new brokers we don't know about yet
        for (String id : brokerIds) {
            if (!clusterMetadata.containsKey(id)) {
                try {
                    String data = zkClient.getData("/brokers/" + id);
                    if (data != null) {
                        String[] parts = data.split(":");
                        clusterMetadata.put(id,
                                new BrokerInfo(Integer.parseInt(id), parts[0], Integer.parseInt(parts[1])));
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Could not load broker info for: " + id, e);
                }
            }
        }

        // Remove brokers that have disappeared
        clusterMetadata.keySet().removeIf(id -> !brokerIds.contains(id));

        // React to the change based on our role
        if (isController.get()) {
            rebalancePartitions(); // we're in charge — fix any broken leader assignments
        } else {
            electController();    // maybe the controller left — try to take over
        }
    }

    // ─────────────────────────────────────────────
    // CONTROLLER ELECTION
    // ─────────────────────────────────────────────

    /**
     * Controller election: first broker to create /controller wins.
     *
     * The /controller node is EPHEMERAL — if the controller crashes,
     * ZooKeeper deletes the node, and all watching brokers immediately
     * try to create it again (stampede). Only one will succeed.
     *
     * If we become controller: rebalancePartitions()
     * If we don't:            watchNode("/controller") so we know when to retry
     *
     * On failure: retry after 2 seconds (ZK might be briefly unavailable)
     */
    private void electController() {
        try {
            String controllerPath = "/controller";

            // Clean up stale controller node with empty data
            // (can happen if previous broker died in the middle of writing)
            boolean nodeExists = zkClient.exists(controllerPath);
            if (nodeExists) {
                String existingData = zkClient.getData(controllerPath);
                if (existingData == null || existingData.trim().isEmpty()) {
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                }
            }

            // Try to become controller
            boolean becameController = false;
            if (!nodeExists) {
                becameController = zkClient.createEphemeralNode(
                        controllerPath, String.valueOf(brokerId));
            }

            if (becameController) {
                isController.set(true);
                LOGGER.info("Broker " + brokerId + " is now the CONTROLLER");
                rebalancePartitions();
            } else {
                isController.set(false);
                LOGGER.info("Broker " + brokerId + " is NOT controller, watching for changes");
                // Watch so we can re-elect if controller dies
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Controller election failed, retrying in 2s", e);
            new Thread(() -> {
                try {
                    Thread.sleep(2000);
                    electController();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }

    /**
     * Called when the /controller node is deleted (controller died)
     * or created (new controller elected).
     *
     * If the node was DELETED → try to become the new controller.
     */
    private void onControllerChange(boolean exists) {
        if (!exists) {
            LOGGER.info("Controller node gone — attempting election");
            electController();
        }
    }

    // ─────────────────────────────────────────────
    // TOPIC MANAGEMENT
    // ─────────────────────────────────────────────

    /**
     * Creates a new topic with the given number of partitions.
     *
     * ONLY THE CONTROLLER can create topics.
     * This avoids two brokers creating conflicting metadata simultaneously.
     *
     * Steps:
     * 1. Create /topics/<name> in ZooKeeper
     * 2. For each partition: pick a leader and followers
     * 3. Create the Partition object and initialize it (creates .log/.index files)
     * 4. Save partition metadata to ZooKeeper (so other brokers can load it)
     * 5. Notify all brokers: "load this topic"
     */
    private void createTopic(String topicName, int numPartitions, short replicationFactor)
            throws Exception {

        if (!isController.get()) {
            LOGGER.warning("Only the controller can create topics");
            return;
        }

        if (topics.containsKey(topicName)) {
            LOGGER.info("Topic already exists: " + topicName);
            return;
        }

        // Persist topic node in ZooKeeper
        zkClient.createPersistentNode("/topics/" + topicName, "");

        Map<Integer, Partition> partitionMap = new ConcurrentHashMap<>();

        List<String> brokerIds = new ArrayList<>(clusterMetadata.keySet());

        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {

            // Round-robin leader assignment across available brokers
            // e.g., partition 0 → broker 0, partition 1 → broker 1, etc.
            int leaderBrokerIndex = partitionId % brokerIds.size();
            int leaderId = Integer.parseInt(brokerIds.get(leaderBrokerIndex));

            // Followers = all OTHER brokers (up to replicationFactor - 1)
            List<Integer> followers = new ArrayList<>();
            for (int i = 1; i < replicationFactor && i < brokerIds.size(); i++) {
                int followerIndex = (leaderBrokerIndex + i) % brokerIds.size();
                followers.add(Integer.parseInt(brokerIds.get(followerIndex)));
            }

            // Create and initialize the Partition object
            String partitionDir = DATA_DIR + File.separator + brokerId
                    + File.separator + topicName + "-" + partitionId;
            Partition partition = new Partition(partitionId, partitionDir);
            partition.setLeader(leaderId);
            partition.setFollowers(followers);
            partition.initialize();

            partitionMap.put(partitionId, partition);

            // Save partition metadata to ZooKeeper
            // Format: "leaderId:follower1,follower2"
            String partitionData = leaderId + ":" + String.join(",",
                    followers.stream().map(String::valueOf).toArray(String[]::new));
            zkClient.createPersistentNode(
                    "/topics/" + topicName + "/partitions/" + partitionId,
                    partitionData);
        }

        topics.put(topicName, partitionMap);
        LOGGER.info("Created topic: " + topicName + " with " + numPartitions + " partitions");

        // Tell all other brokers to load this topic's metadata from ZooKeeper
        notifyBrokersAboutTopic(topicName);
    }

    /**
     * Loads an existing topic's metadata from ZooKeeper into memory.
     * Called on startup (recovery) and when we receive a TOPIC_NOTIFICATION.
     *
     * ZooKeeper is the source of truth. This just builds the in-memory view.
     */
    private void loadTopic(String topicName) throws Exception {
        if (topics.containsKey(topicName)) return; // already loaded

        if (!zkClient.exists("/topics/" + topicName)) {
            LOGGER.warning("Topic not found in ZooKeeper: " + topicName);
            return;
        }

        // Create local directory structure
        File topicDir = new File(DATA_DIR + File.separator + brokerId + File.separator + topicName);
        topicDir.mkdirs();

        Map<Integer, Partition> partitionMap = new ConcurrentHashMap<>();

        // Read each partition's metadata from ZooKeeper
        List<String> partitionIds = zkClient.getChildren("/topics/" + topicName + "/partitions");

        for (String partIdStr : partitionIds) {
            int partitionId = Integer.parseInt(partIdStr);

            String partitionData = zkClient.getData(
                    "/topics/" + topicName + "/partitions/" + partitionId);
            if (partitionData == null) continue;

            // Parse "leaderId:follower1,follower2"
            String[] parts = partitionData.split(":");
            int leaderId = Integer.parseInt(parts[0]);
            List<Integer> followers = new ArrayList<>();
            if (parts.length > 1 && !parts[1].isEmpty()) {
                for (String f : parts[1].split(",")) {
                    followers.add(Integer.parseInt(f));
                }
            }

            String partitionDir = DATA_DIR + File.separator + brokerId
                    + File.separator + topicName + "-" + partitionId;
            Partition partition = new Partition(partitionId, partitionDir);
            partition.setLeader(leaderId);
            partition.setFollowers(followers);
            partition.initialize();

            partitionMap.put(partitionId, partition);
        }

        topics.put(topicName, partitionMap);
        LOGGER.info("Loaded topic: " + topicName);
    }

    /** On startup, reload all topics this broker knew about before restart */
    private void loadExistingTopics() {
        try {
            List<String> topicNames = zkClient.getChildren("/topics");
            for (String topicName : topicNames) {
                loadTopic(topicName);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to load existing topics", e);
        }
    }

    /**
     * Rebalance: scan all partitions and fix any that have a missing/dead leader.
     *
     * This runs when:
     *   - A broker dies (its leader partitions need new leaders)
     *   - This broker becomes the new controller
     *
     * ONLY the controller should call this.
     */
    private void rebalancePartitions() {
        if (!isController.get()) return;

        LOGGER.info("Controller rebalancing partitions...");

        for (Map.Entry<String, Map<Integer, Partition>> topicEntry : topics.entrySet()) {
            String topicName = topicEntry.getKey();

            for (Map.Entry<Integer, Partition> partEntry : topicEntry.getValue().entrySet()) {
                Partition partition = partEntry.getValue();
                int currentLeader = partition.getLeader();

                // If the current leader is no longer in the cluster, assign a new one
                if (!clusterMetadata.containsKey(String.valueOf(currentLeader))) {
                    List<Integer> followers = new ArrayList<>(partition.getFollowers());

                    if (!followers.isEmpty()) {
                        // Promote the first available follower to leader
                        int newLeader = followers.remove(0);
                        partition.setLeader(newLeader);
                        partition.setFollowers(followers);

                        // Persist the new assignment in ZooKeeper
                        try {
                            String newData = newLeader + ":" + String.join(",",
                                    followers.stream().map(String::valueOf).toArray(String[]::new));
                            zkClient.setData("/topics/" + topicName
                                    + "/partitions/" + partition.getId(), newData);
                            LOGGER.info("Reassigned partition " + partition.getId()
                                    + " of topic " + topicName + " to leader " + newLeader);
                        } catch (Exception e) {
                            LOGGER.log(Level.WARNING, "Failed to update partition metadata", e);
                        }
                    }
                }
            }
        }
    }

    // ─────────────────────────────────────────────
    // NETWORKING: ACCEPT CONNECTIONS
    // ─────────────────────────────────────────────

    /**
     * Main connection-accepting loop.
     * Runs in its own thread (submitted to executor in start()).
     *
     * Because serverChannel is non-blocking, accept() returns null immediately
     * when no client is connecting — so we sleep 100ms to avoid spinning the CPU.
     */
    private void acceptConnections() {
        LOGGER.info("Accepting connections on port " + brokerPort);
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    clientChannel.configureBlocking(false);
                    LOGGER.info("New client connected: " + clientChannel.getRemoteAddress());
                    // Handle this client in a separate thread
                    executor.submit(() -> handleClient(clientChannel));
                }
                Thread.sleep(100);
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }

    /**
     * Handles one connected client for the duration of their session.
     * Reads requests in a loop until the client disconnects or we shut down.
     *
     * Buffer is 64KB — enough for a typical produce/fetch request.
     * (Real Kafka uses 1MB+ buffers and configurable max message sizes.)
     */
    private void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();
                int bytesRead = clientChannel.read(buffer);

                if (bytesRead > 0) {
                    buffer.flip(); // switch from write mode to read mode
                    processClientMessage(clientChannel, buffer);
                } else if (bytesRead < 0) {
                    // -1 means the client closed the connection cleanly
                    clientChannel.close();
                    break;
                }
                Thread.sleep(50);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Client connection error", e);
        } finally {
            try {
                if (clientChannel.isOpen()) clientChannel.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing client channel", e);
            }
        }
    }

    /**
     * Reads the first byte (the message type) and dispatches to the right handler.
     *
     * This is the ROUTER of our broker. Every request starts with one byte
     * that tells us what the client wants.
     *
     * Notice REPLICATE and TOPIC_NOTIFICATION here — those come from OTHER BROKERS,
     * not from end-user producers/consumers.
     */
    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {

        byte messageType = buffer.get();

        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type: " + messageType);
        }
    }

    // ─────────────────────────────────────────────
    // REQUEST HANDLERS
    // ─────────────────────────────────────────────

    /**
     * PRODUCE REQUEST FORMAT (after the 1-byte type header):
     * ┌──────────────────────┬────────────────────┬─────────────────┬──────────────────┐
     * │ 2 bytes              │ 4 bytes            │ 4 bytes         │ N bytes          │
     * │ topic name length    │ topic name (UTF-8) │ partition id    │ message payload  │
     * └──────────────────────┴────────────────────┴─────────────────┴──────────────────┘
     *
     * Steps:
     * 1. Parse the request
     * 2. Find the partition
     * 3. Check: am I the leader?
     *    - YES: append to log, replicate to followers, ACK client
     *    - NO:  forward the request to the leader broker
     */
    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {

        // Parse topic name (2-byte length prefix)
        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);

        int partitionId = buffer.getInt();

        // Parse message (remaining bytes)
        byte[] message = new byte[buffer.remaining()];
        buffer.get(message);

        // Validate
        Map<Integer, Partition> partitionMap = topics.get(topicName);
        if (partitionMap == null) {
            Protocol.sendErrorResponse(clientChannel, "Unknown topic: " + topicName);
            return;
        }

        Partition partition = partitionMap.get(partitionId);
        if (partition == null) {
            Protocol.sendErrorResponse(clientChannel, "Unknown partition: " + partitionId);
            return;
        }

        // Am I the leader for this partition?
        if (partition.getLeader() != brokerId) {
            // Forward to the actual leader
            forwardToLeader(partition.getLeader(), topicName, partitionId, message);
            // Tell the client which broker is the leader
            Protocol.sendErrorResponse(clientChannel,
                    "Not leader. Leader is broker " + partition.getLeader());
            return;
        }

        // I am the leader — append to my local log
        long offset = partition.append(message);

        // Replicate to followers asynchronously
        replicateToFollowers(topicName, partition, message, offset);

        // ACK: send the assigned offset back to the producer
        // FORMAT: 1 byte (PRODUCE_RESPONSE) + 8 bytes (offset)
        ByteBuffer response = ByteBuffer.allocate(1 + 8);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.flip();
        clientChannel.write(response);
    }

    /**
     * FETCH REQUEST FORMAT (after the 1-byte type header):
     * ┌──────────────────────┬────────────────────┬──────────────┬──────────────┬──────────────┐
     * │ 2 bytes              │ N bytes            │ 4 bytes      │ 8 bytes      │ 4 bytes      │
     * │ topic name length    │ topic name         │ partition id │ start offset │ max bytes    │
     * └──────────────────────┴────────────────────┴──────────────┴──────────────┴──────────────┘
     *
     * RESPONSE FORMAT:
     * 1 byte (FETCH_RESPONSE) + 4 bytes (message count) +
     *   for each message: 8 bytes (offset) + 4 bytes (length) + N bytes (data)
     */
    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {

        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);

        int partitionId = buffer.getInt();
        long startOffset = buffer.getLong();
        int maxBytes = buffer.getInt();

        Map<Integer, Partition> partitionMap = topics.get(topicName);
        if (partitionMap == null) {
            Protocol.sendErrorResponse(clientChannel, "Unknown topic: " + topicName);
            return;
        }

        Partition partition = partitionMap.get(partitionId);
        if (partition == null) {
            Protocol.sendErrorResponse(clientChannel, "Unknown partition: " + partitionId);
            return;
        }

        // Check the requested offset is not beyond what we have
        if (startOffset >= partition.getNextOffset()) {
            // No messages available yet — send empty response
            ByteBuffer response = ByteBuffer.allocate(1 + 4);
            response.put(Protocol.FETCH_RESPONSE);
            response.putInt(0); // 0 messages
            response.flip();
            clientChannel.write(response);
            return;
        }

        List<byte[]> messages = partition.readMessages(startOffset, maxBytes);

        // Build response: type + count + [offset + length + data] per message
        int responseSize = 1 + 4; // type + count
        for (byte[] msg : messages) responseSize += 8 + 4 + msg.length;

        ByteBuffer response = ByteBuffer.allocate(responseSize);
        response.put(Protocol.FETCH_RESPONSE);
        response.putInt(messages.size());

        long currentOffset = startOffset;
        for (byte[] msg : messages) {
            response.putLong(currentOffset++);
            response.putInt(msg.length);
            response.put(msg);
        }

        response.flip();
        clientChannel.write(response);
    }

    /**
     * METADATA REQUEST: client asks "who is the leader for topic X, partition Y?"
     *
     * Clients need this before they can produce or consume — they need to know
     * which broker to connect to for each partition.
     *
     * RESPONSE: leaderId (4 bytes) + leaderHost (string) + leaderPort (4 bytes)
     */
    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {

        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);

        int partitionId = buffer.getInt();

        Map<Integer, Partition> partitionMap = topics.get(topicName);
        if (partitionMap == null) {
            Protocol.sendErrorResponse(clientChannel, "Unknown topic: " + topicName);
            return;
        }

        Partition partition = partitionMap.get(partitionId);
        if (partition == null) {
            Protocol.sendErrorResponse(clientChannel, "Unknown partition: " + partitionId);
            return;
        }

        BrokerInfo leaderInfo = clusterMetadata.get(String.valueOf(partition.getLeader()));
        if (leaderInfo == null) {
            Protocol.sendErrorResponse(clientChannel, "Leader not found");
            return;
        }

        byte[] hostBytes = leaderInfo.getHost().getBytes();
        ByteBuffer response = ByteBuffer.allocate(
                1 + 4 + 2 + hostBytes.length + 4);  // type + id + hostLen + host + port
        response.put(Protocol.METADATA_RESPONSE);
        response.putInt(leaderInfo.getId());
        response.putShort((short) hostBytes.length);
        response.put(hostBytes);
        response.putInt(leaderInfo.getPort());
        response.flip();
        clientChannel.write(response);
    }

    /**
     * CREATE_TOPIC REQUEST: typically sent by an admin client or producer on first use.
     *
     * FORMAT: topicNameLength (2) + topicName + numPartitions (4) + replicationFactor (2)
     */
    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {

        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);

        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        try {
            createTopic(topicName, numPartitions, replicationFactor);

            // ACK: just send back a 0x01 success byte
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0x01);
            response.flip();
            clientChannel.write(response);

        } catch (Exception e) {
            Protocol.sendErrorResponse(clientChannel, "Failed to create topic: " + e.getMessage());
        }
    }

    /**
     * REPLICATE REQUEST: received from the LEADER broker for a partition.
     *
     * This is broker-to-broker communication.
     * We are a FOLLOWER receiving a copy of a message we need to store locally.
     *
     * FORMAT: topicNameLength (2) + topicName + partitionId (4) + offset (8) + message
     */
    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {

        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);

        int partitionId = buffer.getInt();
        long offset     = buffer.getLong(); // informational — we'll assign our own

        byte[] message = new byte[buffer.remaining()];
        buffer.get(message);

        Map<Integer, Partition> partitionMap = topics.get(topicName);
        if (partitionMap == null || !partitionMap.containsKey(partitionId)) {
            Protocol.sendErrorResponse(clientChannel, "Unknown topic/partition for replication");
            return;
        }

        // Append to our local log (as follower)
        partitionMap.get(partitionId).append(message);

        // ACK back to the leader
        ByteBuffer ack = ByteBuffer.allocate(1);
        ack.put((byte) 0x01);
        ack.flip();
        clientChannel.write(ack);

        LOGGER.info("Replicated message to partition " + partitionId + " of " + topicName);
    }

    /**
     * TOPIC_NOTIFICATION: received from the controller after it creates a new topic.
     *
     * The controller sends this to all other brokers so they can load the
     * new topic's metadata and be ready to receive replicated messages.
     *
     * FORMAT: topicNameLength (2) + topicName
     */
    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer)
            throws IOException {

        short topicLen = buffer.getShort();
        byte[] topicBytes = new byte[topicLen];
        buffer.get(topicBytes);
        String topicName = new String(topicBytes);

        try {
            loadTopic(topicName);
            LOGGER.info("Loaded topic from notification: " + topicName);

            ByteBuffer ack = ByteBuffer.allocate(1);
            ack.put((byte) 0x01);
            ack.flip();
            clientChannel.write(ack);

        } catch (Exception e) {
            Protocol.sendErrorResponse(clientChannel, "Failed to load topic: " + e.getMessage());
        }
    }

    // ─────────────────────────────────────────────
    // REPLICATION
    // ─────────────────────────────────────────────

    /**
     * After writing a message as the leader, we push a copy to every follower.
     *
     * Each follower gets its own thread so we don't block if one is slow.
     * (Real Kafka uses async replication with ISR — In-Sync Replicas — tracking.)
     *
     * We open a fresh TCP connection per replication call here.
     * A real implementation would keep persistent connections to followers.
     */
    private void replicateToFollowers(String topicName, Partition partition,
                                      byte[] message, long offset) {

        for (int followerId : partition.getFollowers()) {
            executor.submit(() -> {
                BrokerInfo follower = clusterMetadata.get(String.valueOf(followerId));
                if (follower == null) return;

                try (SocketChannel channel = SocketChannel.open(
                        new InetSocketAddress(follower.getHost(), follower.getPort()))) {

                    byte[] topicBytes = topicName.getBytes();

                    // Build replication request
                    ByteBuffer req = ByteBuffer.allocate(
                            1 + 2 + topicBytes.length + 4 + 8 + message.length);
                    req.put(Protocol.REPLICATE);
                    req.putShort((short) topicBytes.length);
                    req.put(topicBytes);
                    req.putInt(partition.getId());
                    req.putLong(offset);
                    req.put(message);
                    req.flip();

                    channel.write(req);

                    // Wait for ACK
                    ByteBuffer ack = ByteBuffer.allocate(1);
                    channel.read(ack);

                    LOGGER.info("Replicated offset " + offset + " to broker " + followerId);

                } catch (IOException e) {
                    LOGGER.log(Level.WARNING,
                            "Replication to broker " + followerId + " failed", e);
                }
            });
        }
    }

    /**
     * Forwards a produce request to the actual leader broker.
     *
     * This happens when a producer connects to a non-leader broker for a partition.
     * We act as a proxy: forward the request to the leader, then tell the client to redirect.
     */
    private void forwardToLeader(int leaderId, String topicName, int partitionId, byte[] message) {
        BrokerInfo leader = clusterMetadata.get(String.valueOf(leaderId));
        if (leader == null) return;

        try (SocketChannel channel = SocketChannel.open(
                new InetSocketAddress(leader.getHost(), leader.getPort()))) {

            byte[] topicBytes = topicName.getBytes();
            ByteBuffer req = ByteBuffer.allocate(
                    1 + 2 + topicBytes.length + 4 + message.length);
            req.put(Protocol.PRODUCE);
            req.putShort((short) topicBytes.length);
            req.put(topicBytes);
            req.putInt(partitionId);
            req.put(message);
            req.flip();

            channel.write(req);

        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to forward to leader " + leaderId, e);
        }
    }

    /**
     * Controller sends TOPIC_NOTIFICATION to all known brokers.
     * Called after createTopic() completes.
     */
    private void notifyBrokersAboutTopic(String topicName) {
        byte[] topicBytes = topicName.getBytes();

        for (Map.Entry<String, BrokerInfo> entry : clusterMetadata.entrySet()) {
            int targetId = Integer.parseInt(entry.getKey());
            if (targetId == brokerId) continue; // don't notify ourselves

            BrokerInfo target = entry.getValue();
            try (SocketChannel channel = SocketChannel.open(
                    new InetSocketAddress(target.getHost(), target.getPort()))) {

                ByteBuffer req = ByteBuffer.allocate(1 + 2 + topicBytes.length);
                req.put(Protocol.TOPIC_NOTIFICATION);
                req.putShort((short) topicBytes.length);
                req.put(topicBytes);
                req.flip();
                channel.write(req);

            } catch (IOException e) {
                LOGGER.log(Level.WARNING,
                        "Failed to notify broker " + targetId + " about topic " + topicName, e);
            }
        }
    }

    // ─────────────────────────────────────────────
    // MAIN
    // ─────────────────────────────────────────────

    public static void main(String[] args) {
        try {
            SimpleKafkaBroker broker = new SimpleKafkaBroker(1, "localhost", 9092, 2181);
            broker.start();

            // Keep running until Ctrl+C
            Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));

            // Block the main thread
            Thread.currentThread().join();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}