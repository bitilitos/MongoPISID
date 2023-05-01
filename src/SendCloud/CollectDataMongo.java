package SendCloud;

import Mongo.*;
import com.mongodb.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class CollectDataMongo extends Thread {
    private MongoClient mongoClient ;
    private DBCollection mongocol;

    private DB database ;
    private String mongoCollection;

    public BlockingQueue<String> data;
    String mongo_replica;
    String mongo_address;
    String mongo_authentication;

    Timestamp timestampOfLastSent;



    public CollectDataMongo(BlockingQueue<String> messageQueue, String mongoCollection, String mongo_replica, String mongo_authentication, String mongo_address, Timestamp experienceStart) {
        this.mongoCollection = mongoCollection;
        this.data = messageQueue;
        this.mongo_replica = mongo_replica;
        this.mongo_authentication = mongo_authentication;
        this.mongo_address = mongo_address;
        this.timestampOfLastSent = experienceStart;

    }


    @Override
    public void run() {
        try {
            connectMongo(mongoCollection);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(CloudToMongo.getExperienceBeginning() != null)
            getDataFromMongo();

    }

    private void getDataFromMongo () {
        BasicDBObject query = new BasicDBObject();
        query.put("Hour", new BasicDBObject("$gt", timestampOfLastSent.toString()));
        DBCursor iterDoc = mongocol.find(query);
        Iterator it = iterDoc.iterator();
        while (it.hasNext()) {
            String reading = it.next().toString();
            System.out.println("Reading took from Mongo:" + reading);
            data.add(reading);
            DBObject json = CloudToMongo.getDBObjectFromReading(reading);
            if (timestampOfLastSent.before(Timestamp.valueOf((String) json.get("Hour")))) {
                System.out.println(json.get("Hour"));
                timestampOfLastSent = Timestamp.valueOf((String) json.get("Hour"));
            }
        }
        try {
            sleep(1000);
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
