/* 
* This code ties everything together:
* Start broker
* connect to zookeeper
* Register itself
* listen to cluster changes
*/

package com.simplekafka.broker;

import java.util.List;


public class SimpleKafkaBroker{

    private final int brokerId;
    private final String host;
    private final int port;

    private ZooKeeperClient zkClient;

    public SimpleKafkaBroker(int brokerId, String host, int port){
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
    }

    public void start() throws Exception{
        zkClient = new ZooKeeperClient();
        zkClient.connect();

        registerBroker();
        watchBroker();
        electController();
        watchChildren();
    }

    //Registering broker
    private void registerBroker() throws Exception{
        String path = "/brokers/" + brokerId;
        String data = host + ":" + port;

        zkClient.createEphemeralNode(path,data);
        System.out.println("Broker registered: "+ path);
    }

    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    // Watching child nodes
    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    watchChildren(path, callback); // re-register watcher
                }
            });

            callback.onChildrenChanged(children);

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
        }
    }

    //watcher for brokers
    //this shows which brokers are alive
    private void watchBroker(){
        zkClient.watchChildren("/brokers", children -> {
            System.out.println("Active brokers: " + children);
        });
    }
    
    
    //controller election - kafka always has one controller broker
    //logic -> Node created - you are controller
    //      -> Node exists  - Someone else is controller
    //this controller is responsible for partition assignment, Leader election, Cluster decisions
    private void electController() throws Exception{
        boolean isController = zkClient.createEphemeralNode("/controller", String.valueOf(brokerId));
        if(isController){
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