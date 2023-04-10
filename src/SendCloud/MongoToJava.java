package SendCloud;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.swing.*;
import java.io.FileInputStream;
import java.util.*;


public class MongoToJava {
    private MongoClient mongoClient = new MongoClient("localhost", 23019);
    static Map<String, String> collectionsToTablesMap = new HashMap<>();

    private MongoDatabase connectToMongoDB () {
        return mongoClient.getDatabase("data");
    }

    private List<String> getDataFromMongo (MongoDatabase database, String collectionName) {
        List<String> data = new ArrayList<>();
        MongoCollection<Document> collection = database.getCollection(collectionName);
        FindIterable<Document> iterDoc = collection.find();
        Iterator it = iterDoc.iterator();
        while (it.hasNext()) {
            data.add(it.next().toString());
        }
        return data;
    }


    private static void setCollectionsToTablesMap() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("SendCloud.ini"));
            String mongoCollections = p.getProperty("mongo_collections");
            String cloud_topics = p.getProperty("cloud_topics");
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
            Runnable thread = new Runnable() {
                @Override
                public void run() {
                    MongoToJava mongoToJava = new MongoToJava();
                    MongoDatabase database = mongoToJava.connectToMongoDB();
                    List<String> data = mongoToJava.getDataFromMongo(database, collection.getKey());
                    SendCloud publishTopic = new SendCloud();
                    publishTopic.connecCloud(collection.getValue());
                    publishTopic.readData(data);
                }
            };
            thread.run();
        }

    }
}