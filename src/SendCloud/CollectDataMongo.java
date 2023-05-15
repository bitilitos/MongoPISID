package SendCloud;

import Mongo.*;
import com.mongodb.*;
import com.mongodb.DBObject;
import org.bson.types.*;

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

    Timestamp experienceStart;
    ObjectId lastReadingObjectId = null;
    private static boolean isUrgentSet = false;
    private boolean isUrgent = false;




    public CollectDataMongo(BlockingQueue<String> messageQueue, String mongoCollection, String mongo_replica, String mongo_authentication, String mongo_address, Timestamp experienceStart) {
        this.mongoCollection = mongoCollection;
        this.data = messageQueue;
        this.mongo_replica = mongo_replica;
        this.mongo_authentication = mongo_authentication;
        this.mongo_address = mongo_address;
        this.experienceStart = experienceStart;
        if (mongoCollection.equals("alert")&& !isUrgentSet) {
            this.isUrgent = true;
            isUrgentSet = true;
        }
    }


    @Override
    public void run() {
        try {
            connectMongo(mongoCollection);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(CloudToMongo.getExperienceBeginning() != null)
           if (mongocol.getName().equals("alert"))
               getDataFromMongoAlertThread();
           else
            getDataFromMongo();
    }

    private void getDataFromMongo () {
        BasicDBObject query = new BasicDBObject();
        if (lastReadingObjectId == null) {
            query.put("Hour", new BasicDBObject("$gt", experienceStart.toString()));
        }
        else{
            query.put("_id", new BasicDBObject("$gt", lastReadingObjectId));
        }

        DBCursor iterDoc = mongocol.find(query);
        Iterator it = iterDoc.iterator();
        while (it.hasNext()) {
            String reading = it.next().toString();
            System.out.println("Reading took from Mongo:" + reading);
            data.add(reading);
            DBObject json = CloudToMongo.getDBObjectFromReading(reading);
            ObjectId readingID = (ObjectId) json.get("_id");

            if (lastReadingObjectId == null || lastReadingObjectId.compareTo(readingID) < 0) {
                lastReadingObjectId = readingID;
            }
        }
        try {
            sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private void getDataFromMongoAlertThread () {
        BasicDBObject query = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        List<String> urgentAlertList = new ArrayList<String>();
        urgentAlertList.add("High");
        urgentAlertList.add("Very High");

        if (lastReadingObjectId == null)
            obj.add(new BasicDBObject("Hour", new BasicDBObject("$gt", experienceStart.toString())));
        else
            obj.add(new BasicDBObject("_id", new BasicDBObject("$gt", lastReadingObjectId)));

        if (!isUrgent)
            obj.add(new BasicDBObject("AlertType", new BasicDBObject("$nin", urgentAlertList)));
        else
            obj.add(new BasicDBObject("AlertType", new BasicDBObject("$in", urgentAlertList)));

        query.put("$and", obj);


        DBCursor iterDoc = mongocol.find(query);
        Iterator it = iterDoc.iterator();
        while (it.hasNext()) {
            String reading = it.next().toString();
            System.out.println("Reading took from Mongo:" + reading);
            if (!isUrgent)
                data.add(reading);
            else
                SendCloud.publishSensor(reading, "g7_alert");

            DBObject json = CloudToMongo.getDBObjectFromReading(reading);
            ObjectId readingID = (ObjectId) json.get("_id");

            if (lastReadingObjectId == null || lastReadingObjectId.compareTo(readingID) < 0) {
                lastReadingObjectId = readingID;
            }
        }
        try {
            if(!isUrgent)
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
