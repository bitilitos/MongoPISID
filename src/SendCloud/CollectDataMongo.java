package SendCloud;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class CollectDataMongo extends Thread {
    private MongoClient mongoClient = new MongoClient("localhost", 23019);
    private MongoDatabase connectToMongoDB () {
        return mongoClient.getDatabase("data");
    }
    private MongoDatabase database = connectToMongoDB();
    private String mongoCollection;

    public BlockingQueue<String> getData() {
        return data;
    }

    public BlockingQueue<String> data;

    public CollectDataMongo(BlockingQueue<String> messageQueue, String mongoCollection) {
        this.mongoCollection = mongoCollection;
        this.data = messageQueue;
    }


    @Override
    public void run() {
        while(true) getDataFromMongo();

    }

    private void getDataFromMongo () {
        MongoCollection<Document> collection = database.getCollection(mongoCollection);
        FindIterable<Document> iterDoc = collection.find();
        Iterator it = iterDoc.iterator();
        while (it.hasNext()) {
            data.add(it.next().toString());
        }
    }
}
