package SendCloud;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.swing.*;
import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static Mongo.CloudToMongo.*;


public class MongoToJava {
    static String mongoCollections;
    static String cloud_topics;
    static String mongo_authentication;
    static String mongo_database;
    static String mongo_replica;
    static String mqtt_broker;
    private MongoClient mongoClient = new MongoClient("localhost", 23019);
    static Map<String, String> collectionsToTablesMap = new HashMap<>();

    private MongoDatabase connectToMongoDB () {
        return mongoClient.getDatabase("data");
    }

    private static void setCollectionsToTablesMap() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("SendCloud.ini"));
             mongoCollections = p.getProperty("mongo_collections");
             cloud_topics = p.getProperty("cloud_topics");
             mongo_authentication = p.getProperty("mongo_authentication");
             mongo_database = p.getProperty("mongo_database");
             mongo_replica = p.getProperty("mongo_replica");
             mqtt_broker = p.getProperty("cloud_server");
            if (mongoCollections.contains(",")){
                String[] mongoCollections_vector = mongoCollections.split(",");
                String[] mqttTopics_vector = cloud_topics.split(",");
                for (int i = 0; i < mongoCollections_vector.length; i++) {
                    collectionsToTablesMap.put(mongoCollections_vector[i].trim(), mqttTopics_vector[i].trim());
                }
            } else {
                collectionsToTablesMap.put(mongoCollections,cloud_topics);
            }
        } catch (Exception e) {
            System.out.println("Error reading SendCloud.ini file " + e);
            JOptionPane.showMessageDialog(null, "The SendCloud ini file wasn't found.", "Data Migration", JOptionPane.ERROR_MESSAGE);
        }
    }

    public static void main(String[] args) {
        setCollectionsToTablesMap();
        for (Map.Entry<String, String> collection : collectionsToTablesMap.entrySet()){
            BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
            CollectDataMongo collectDataMongo = new CollectDataMongo(messageQueue, collection.getKey(), mongo_replica, mongo_database, mongo_authentication);
            SendCloud publishTopic = new SendCloud(messageQueue, collection.getValue());
            collectDataMongo.start();
            publishTopic.start();
        }

    }
}