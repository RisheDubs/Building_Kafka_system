package com.simplekafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZooKeeperClient implements Watcher {

    private static final Logger LOGGER = Logger.getLogger(ZooKeeperClient.class.getName());
    private static final int SESSION_TIMEOUT = 3000;

    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    private String getConnectionString() {
        return "localhost:2181";
    }

    // CONNECT
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(getConnectionString(), SESSION_TIMEOUT, this);
        connectedSignal.await();

        createPersistentNode("/brokers", "");
        createPersistentNode("/topics", "");
        createPersistentNode("/controller", "");
    }

    // CREATE PERSISTENT NODE
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

    // CREATE EPHEMERAL NODE
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

    // WATCHER (SESSION EVENTS)
    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            LOGGER.info("Connected to ZooKeeper");
        }
    }
}