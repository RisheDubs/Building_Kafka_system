package com.simplekafka.broker;

/*
*Holds information about a broker in the simplkafka cluster
*/

public class BrokerInfo {
    private final int id;
    private final String host;
    private final int port;

    public BrokerInfo(int id,String host, int port){
        this.id = id; //this refers to current object
        this.host = host;
        this.port = port;
    }

    //getter methods to return broker id,host,port
    //since fields are private other classes can't access them directly so we use getters
    public int getId(){
        return id;
    }

    public String getHost(){
        return host;
    }

    public int getPort(){
        return port;
    }

    /*
    * All java classes inherit from java.lang.object, but object has toString(), equals(), hahsCode(), we are just overriding them
    */
    @Override //override tells this method overrides a method from the parent class
    public String toString(){ //toString, defines how object is printed as text
        return "BrokerInfo{id="+ id + ", host ='" + host + "', port =" + port +"}";
    }

    //this method checks if 2 objects represent the same broker
    @Override
    public boolean equals(Object obj){
        if(this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        BrokerInfo other = (BrokerInfo) obj; //type casting
        return id == other.id;
    }

    @Override 
    public int hashCode(){ //generate a numeric hash value for the object
        return Integer.hashCode(id);//compares broker using id
    }
}