package SendCloud;

import com.mongodb.*;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

public class CollectDataMongo extends Thread {
    private MongoClient mongoClient ;
    private DBCollection mongocol;

    private DB database ;
    private String mongoCollection;

    public BlockingQueue<String> data;
    String mongo_replica;
    String mongo_address;
    String mongo_authentication;



    public CollectDataMongo(BlockingQueue<String> messageQueue, String mongoCollection, String mongo_replica, String mongo_authentication, String mongo_address) {
        this.mongoCollection = mongoCollection;
        this.data = messageQueue;
        this.mongo_replica = mongo_replica;
        this.mongo_authentication = mongo_authentication;
        this.mongo_address = mongo_address;

    }


    @Override
    public void run() {
        try {
            connectMongo(mongoCollection);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(true) getDataFromMongo();

    }

    private void getDataFromMongo () {
        BasicDBObject query = new BasicDBObject();
        query.put("Hour", new BasicDBObject("$gt", "2023-04-28 11:09:37.220358"));
        DBCursor iterDoc = mongocol.find(query);
        Iterator it = iterDoc.iterator();
        while (it.hasNext()) {
            String reading = it.next().toString();
            data.add(reading);
           System.out.println("Took data from Mongo" + reading);
           System.out.println("dataStru: " + data.hashCode() + Thread.currentThread() + " Collection " + mongocol.getName());
        }
        try {
            sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void connectMongo(String collection) {
        String mongoURI = new String();
        mongoURI = "mongodb://";
        mongoURI = mongoURI + mongo_address;
        if (!mongo_replica.equals("false"))
            if (mongo_authentication.equals("true")) mongoURI = mongoURI + "/?replicaSet=" + mongo_replica+"&authSource=admin";
            else mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;
        else
        if (mongo_authentication.equals("true")) mongoURI = mongoURI  + "/?authSource=admin";
        mongoClient = new MongoClient((new MongoClientURI(mongoURI)));
        database = mongoClient.getDB("data");
        mongocol = database.getCollection(collection);
        if(mongocol!=null){
            System.out.println("Sucesso");

        }


    }
}
