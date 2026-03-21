/* 
* This code ties everything together:
* Start broker
* connect to zookeeper
* Register itself
* listen to cluster changes
*/

package com.simplekafka.broker;

public class SimpleKafkaBroker{

    private final int brokerId;
    private final String host;
    private final int port;

    private ZooKeeperClient zkClient;

    public SimpleKafkaBroker(int brokerId, String host, int port) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
    }

    public void start() throws Exception {
        zkClient = new ZooKeeperClient();
        zkClient.connect();

        registerBroker();
        watchBrokers();
        electController();
    }

    // Register broker in ZooKeeper
    private void registerBroker() throws Exception {
        String path = "/brokers/" + brokerId;
        String data = host + ":" + port;

        zkClient.createEphemeralNode(path, data);
        System.out.println("Broker registered: " + path);
    }

    // Watch active brokers
    private void watchBrokers() {
        zkClient.watchChildren("/brokers", children -> {
            System.out.println("Active brokers: " + children);
        });
    }

    //controller election - kafka always has one controller broker
    //logic -> Node created - you are controller
    //      -> Node exists  - Someone else is controller
    //this controller is responsible for partition assignment, Leader election, Cluster decisions

    // Controller election
    private void electController() throws Exception {
        boolean isController = zkClient.createEphemeralNode(
                "/controller",
                String.valueOf(brokerId)
        );

        if (isController) {
            System.out.println("I am the controller");
        } else {
            System.out.println("Another broker is controller");
        }
    }

    //Main method entry point
    public static void main(String[] args) {
        try {
            SimpleKafkaBroker broker =
                    new SimpleKafkaBroker(1, "localhost", 9092);

            broker.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}