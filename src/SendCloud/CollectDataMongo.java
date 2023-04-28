package SendCloud;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static Mongo.CloudToMongo.mongo_authentication;

public class CollectDataMongo extends Thread {
    private MongoClient mongoClient ;
    private DBCollection mongocol;
    private MongoDatabase connectToMongoDB () {
        return mongoClient.getDatabase("data");
    }
    private DB database ;
    private String mongoCollection;

    public BlockingQueue<String> getData() {
        return data;
    }

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
        DBCursor iterDoc = mongocol.find();
        Iterator it = iterDoc.iterator();
        while (it.hasNext()) {
            data.add(it.next().toString());
            System.out.println("Took data from Mongo" + data.toString());
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
