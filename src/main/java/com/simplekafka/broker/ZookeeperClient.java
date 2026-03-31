package com.simplekafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WHAT IS THIS CLASS?
 *
 * ZooKeeperClient is our wrapper around Apache ZooKeeper — the coordination
 * service that keeps all brokers in sync.
 *
 * Think of ZooKeeper as a shared whiteboard that all brokers can read/write.
 * It's where they announce themselves, elect a controller, and store topic metadata.
 *
 * STAGE 5 ADDITIONS vs STAGE 3:
 * Stage 3 had: connect, createPersistentNode, createEphemeralNode, watchChildren
 * Stage 5 adds:
 *   - watchNode()    → watch a SINGLE node (used to detect when controller dies)
 *   - exists()       → check if a path exists (returns boolean)
 *   - getData()      → read the data stored at a ZK path
 *   - setData()      → update data at a ZK path
 *   - deleteNode()   → delete a ZK node
 *   - getChildren()  → list child nodes at a path
 *
 * These new methods are needed because the broker now does more:
 *   - Stores topic/partition metadata in ZooKeeper
 *   - Reads metadata back on startup
 *   - Watches the /controller node for re-election
 */
public class ZooKeeperClient implements Watcher {

    private static final Logger LOGGER = Logger.getLogger(ZooKeeperClient.class.getName());
    private static final int SESSION_TIMEOUT = 3000;

    private ZooKeeper zooKeeper;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);
    private final String host;
    private final int port;

    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    public interface NodeCallback {
        void onNodeChanged(boolean exists);
    }

    public ZooKeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    // ─────────────────────────────────────────────
    // CONNECT
    // ─────────────────────────────────────────────

    /**
     * Establishes a ZooKeeper session and creates the root nodes if they don't exist.
     * CountDownLatch makes us WAIT here until ZooKeeper confirms we're connected.
     * Without this, we'd race ahead and try to create nodes before the connection is ready.
     */
    public void connect() throws IOException, InterruptedException, KeeperException {
        zooKeeper = new ZooKeeper(host + ":" + port, SESSION_TIMEOUT, this);
        connectedSignal.await(); // block until process() fires with SyncConnected

        // Ensure root paths exist (safe to call repeatedly — checks before creating)
        createPersistentNode("/brokers", "");
        createPersistentNode("/topics", "");
    }

    // ─────────────────────────────────────────────
    // SESSION WATCHER
    // ─────────────────────────────────────────────

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown(); // unblock connect()
            LOGGER.info("Connected to ZooKeeper");
        }
    }

    // ─────────────────────────────────────────────
    // CREATE NODES
    // ─────────────────────────────────────────────

    /**
     * Creates a PERSISTENT node — survives broker restarts and session loss.
     * Used for: /topics/..., /brokers/...(metadata), root paths
     */
    public void createPersistentNode(String path, String data)
            throws KeeperException, InterruptedException {

        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            LOGGER.info("Created persistent node: " + path);
        } else {
            zooKeeper.setData(path, data.getBytes(), -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    /**
     * Creates an EPHEMERAL node — automatically deleted when the ZK session ends
     * (i.e., when the broker crashes or disconnects).
     *
     * This is the key mechanism for:
     *   - Broker registration (/brokers/1) — disappears when broker dies
     *   - Controller election (/controller) — first writer wins
     *
     * Returns true if we created the node (we "won"), false if it already existed.
     */
    public boolean createEphemeralNode(String path, String data)
            throws KeeperException, InterruptedException {

        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            LOGGER.info("Created ephemeral node: " + path);
            return true;
        }
        return false;
    }

    // ─────────────────────────────────────────────
    // READ / CHECK NODES  (NEW IN STAGE 5)
    // ─────────────────────────────────────────────

    /**
     * Returns true if the given ZooKeeper path exists.
     * Used in electController() to check if /controller is already taken.
     */
    public boolean exists(String path) throws KeeperException, InterruptedException {
        return zooKeeper.exists(path, false) != null;
    }

    /**
     * Reads the string data stored at a ZooKeeper path.
     * Used to read broker addresses, partition metadata, etc.
     *
     * Returns null if the node doesn't exist.
     */
    public String getData(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) return null;

        byte[] data = zooKeeper.getData(path, false, stat);
        return data != null ? new String(data) : null;
    }

    /**
     * Updates the data at an existing ZooKeeper path.
     * The -1 means "update regardless of version" (no optimistic locking).
     * Used when partition leaders change during rebalancing.
     */
    public void setData(String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data.getBytes(), -1);
        LOGGER.info("Updated node data: " + path);
    }

    /**
     * Deletes a ZooKeeper node.
     * Used in electController() to clean up a stale /controller node
     * that has empty data (left over from a bad shutdown).
     */
    public void deleteNode(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat != null) {
            zooKeeper.delete(path, -1); // -1 = delete any version
            LOGGER.info("Deleted node: " + path);
        }
    }

    /**
     * Returns a list of child node names at a given path.
     * Used to: list all brokers (/brokers), list all topics (/topics), etc.
     *
     * e.g., getChildren("/brokers") → ["1", "2", "3"]
     */
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        return zooKeeper.getChildren(path, false);
    }

    // ─────────────────────────────────────────────
    // WATCHES  (NEW: watchNode)
    // ─────────────────────────────────────────────

    /**
     * Watches CHILD NODES of a path.
     * Fires when a broker joins or leaves the cluster.
     * We re-register the watch on every event (ZK watches are one-shot).
     */
    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    watchChildren(path, callback); // re-register
                }
            });
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children for: " + path, e);
        }
    }

    /**
     * NEW IN STAGE 5: Watches a SINGLE node for existence changes.
     *
     * WHY THIS IS NEEDED:
     * When a non-controller broker starts, it loses the /controller election.
     * But it needs to know if the current controller DIES — so it can try to
     * become the new controller.
     *
     * watchNode("/controller", exists -> { if (!exists) electController(); })
     *
     * The callback fires with:
     *   exists = false  → node was deleted (controller died)
     *   exists = true   → node was created (new controller elected)
     *
     * Again, ZK watches are one-shot, so we re-register inside the callback.
     */
    public void watchNode(String path, NodeCallback callback) {
        try {
            Stat stat = zooKeeper.exists(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    callback.onNodeChanged(false); // node gone
                    watchNode(path, callback);     // re-register for next change
                } else if (event.getType() == Watcher.Event.EventType.NodeCreated) {
                    callback.onNodeChanged(true);  // node came back
                    watchNode(path, callback);
                }
            });
            // Initial state: callback fires immediately with current existence
            //callback.onNodeChanged(stat != null); <-bug
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }

    // ─────────────────────────────────────────────
    // CLOSE
    // ─────────────────────────────────────────────

    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
            LOGGER.info("ZooKeeper connection closed");
        }
    }
}