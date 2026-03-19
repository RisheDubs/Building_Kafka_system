/*
The wire protocol is the binary format that brokers and clients use to communicate. 
The Protocol class is the foundation of Build Your Own Kafka's wire protocol implementation, 
defining how clients and brokers communicate over the network.
Kafka uses a binary protocol for efficiency. Each message type is identified by a unique byte code, 
followed by the message payload. This approach is more efficient than text-based protocols like HTTP 
for high-throughput messaging.
*/

// Client request types
public static final byte PRODUCE = 0x01;
public static final byte FETCH = 0x02;
public static final byte METADATA = 0x03;
public static final byte CREATE_TOPIC = 0x04;
    
// Broker response types
public static final byte PRODUCE_RESPONSE = 0x11;
public static final byte FETCH_RESPONSE = 0x12;
