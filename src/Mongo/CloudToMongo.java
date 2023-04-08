package Mongo;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.eclipse.paho.client.mqttv3.*;
import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class CloudToMongo implements MqttCallback {

    private MqttClient mqttclient;



    static MongoClient mongoClient;


    static DB db;
    private DBCollection mongocol;
    static String mongo_user = new String();
    static String mongo_password = new String();
    static String mongo_address = new String();
    static String cloud_server = new String();
    String cloud_topic = new String();
    static String mongo_host = new String();
    static String mongo_replica = new String();
    static String mongo_database = new String();
    static String mongo_collection = new String();
    static String mongo_authentication = new String();
    static JTextArea documentLabel = new JTextArea("\n");
    static Map<String, String> topics = new HashMap<>();
    private BlockingQueue <String> readingsForMongo = new LinkedBlockingQueue<>();


    // Flag -> Waiting for Experience Reading to arrive
    private static boolean isWaitingForExperienceStart = true;
    // Flag -> Message indicating experience start has arrived
    private static boolean hasStartReadingArrived = false;

    private static boolean experienceMustEnd = false;


    private static Timestamp experienceBeginning = null;

    private static File csvFile = new File("inserts.csv");
    private static FileWriter fw;

    private static void createWindow() {
        JFrame frame = new JFrame("Cloud to Mongo");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JLabel textLabel = new JLabel("Data from broker: ",SwingConstants.CENTER);
        textLabel.setPreferredSize(new Dimension(600, 30));
        JScrollPane scroll = new JScrollPane (documentLabel, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        scroll.setPreferredSize(new Dimension(600, 200));
        JButton b1 = new JButton("Stop the program");
        frame.getContentPane().add(textLabel, BorderLayout.PAGE_START);
        frame.getContentPane().add(scroll, BorderLayout.CENTER);
        frame.getContentPane().add(b1, BorderLayout.PAGE_END);
        frame.setLocationRelativeTo(null);
        frame.pack();
        frame.setVisible(true);
        b1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                System.exit(0);
            }
        });
    }

    public static void initiate() {

        createWindow();

        try {
            Properties p = new Properties();
            p.load(new FileInputStream("CloudToMongo.ini"));
            String cloud_topics = p.getProperty("cloud_topic");
            mongo_collection = p.getProperty("mongo_collection");
            topics = topicsList(cloud_topics, mongo_collection);

            mongo_address = p.getProperty("mongo_address");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");
            mongo_replica = p.getProperty("mongo_replica");
            cloud_server = p.getProperty("cloud_server");

            mongo_host = p.getProperty("mongo_host");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");

        } catch (Exception e) {
            System.out.println("Error reading CloudToMongo.ini file " + e);
            JOptionPane.showMessageDialog(null, "The CloudToMongo.inifile wasn't found.", "CloudToMongo", JOptionPane.ERROR_MESSAGE);
        }

        //**************************//
        // for testing purposes only
        // experienceBeginning = new Timestamp(System.currentTimeMillis());

        for (Map.Entry<String, String> topic : topics.entrySet()){
            Runnable thread = new Runnable() {
                @Override
                public void run() {
                    CloudToMongo cloudToMongo = new CloudToMongo();
                    cloudToMongo.connectCloud(topic.getKey());
                    cloudToMongo.connectMongo(topic.getValue());
                    QueueToMongo queueToMongo = new QueueToMongo(cloudToMongo.mongocol, cloudToMongo.readingsForMongo);
                    queueToMongo.start();
                }
            };
            thread.run();
        }

    }

    private void insertToQueue (DBCollection mongoCol, String topic, String reading) {
        readingsForMongo.add(reading);
        String insert = "Queue Insert, " + topic + "," + " " + reading +",";
        if (fw != null) {
            try {
                fw.append(insert + "\n");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println(insert);

    }


    private static Map<String, String> topicsList(String cloud_topic, String collections) {
        Map <String, String> result = new HashMap<>();
        if (cloud_topic.contains(",")){
            String[] topic_vector = cloud_topic.split(",");
            String[] collections_vector = collections.split(",");
            for (int i = 0; i < topic_vector.length; i++) {
                result.put(topic_vector[i].trim(), collections_vector[i].trim());
            }
            return result;
        } else {
            result.put(cloud_topic,collections);
            return result;
        }
    }


    public void connectCloud(String topic) {
        cloud_topic = topic;
        int i;
        try {
            i = new Random().nextInt(100000);
            mqttclient = new MqttClient(cloud_server, "CloudToMongo_"+String.valueOf(i)+"_"+cloud_topic);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void connectMongo(String collection) {
        String mongoURI = new String();

        mongoURI = "mongodb://";

        if (mongo_authentication.equals("true")) mongoURI = mongoURI + mongo_user + ":" + mongo_password + "@";
        mongoURI = mongoURI + mongo_address;
        if (!mongo_replica.equals("false"))
            if (mongo_authentication.equals("true")) mongoURI = mongoURI + "/?replicaSet=" + mongo_replica+"&authSource=admin";
            else mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;
        else
        if (mongo_authentication.equals("true")) mongoURI = mongoURI  + "/?authSource=admin";
        MongoClient mongoClient = new MongoClient((new MongoClientURI(mongoURI)));
        db = mongoClient.getDB(mongo_database);
        mongocol = db.getCollection(collection);
        if(mongocol!=null){
            System.out.println("Sucesso");

        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage c)
            throws Exception {
        try {
            documentLabel.insert(c.toString()+"\n", 0);
            //**************************//
            // for testing purpose only
            // insertToQueue(mongocol, topic, c.toString());
            DBObject json = getDBObjectFromReading(c.toString());

                // To get Start Message when expecting experience to Start
                if (isWaitingForExperienceStart) {
                    if (json != null) {
                        if (isReadingExperienceStart(json)) insertToQueue(mongocol, topic, c.toString());
                        return;
                    }
                }


                // To get first timeStamp after start experience message
                if (hasStartReadingArrived && experienceBeginning == null) {
                    if (json != null) {
                        String timestamp = json.get("Hora").toString();
                        if (timestamp != null && !timestamp.isEmpty()) startExperience(timestamp);
                        insertToQueue(mongocol, topic, c.toString());
                    }
                }

                // When experience is running to get start of new experience
                if (experienceBeginning != null)  {
                    if (json != null) {
                        isReadingExperienceStart(json);
                    }
                    insertToQueue(mongocol, topic, c.toString());
                }

        } catch (Exception e) {
            System.out.println(e);
        }
    }
    public BlockingQueue<String> getReadingsForMongo() {
        return readingsForMongo;
    }

    public static Map<String, String> getTopics() {return topics;}

    @Override
    public void connectionLost(Throwable cause) {
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {

    }


    public static void main(String[] args) {
        CloudToMongo.initiate();

    }

    // checks if reading is experience start
    private boolean isReadingExperienceStart (DBObject json) {
        if (json==null) return false;
        if (json.get("Hora").toString().equals("2000-01-01 00:00:00") && json.get("SalaEntrada").toString().equals("0") &&
                json.get("SalaSaida").toString().equals("0")) {
            if (isWaitingForExperienceStart) {
                hasStartReadingArrived = true;
                isWaitingForExperienceStart = false;
                System.out.println("IMPORTANT -> EXPERIENCE STARTED!!");
            }else if (experienceBeginning != null) {
                experienceMustEnd = true;
                System.out.println("IMPORTANT -> NEW EXPERIENCE STARTED, MUST END THIS ONE!!");

            }
            return true;

        }
        return false;
    }

    private void startExperience(String timestamp) {
        experienceBeginning = Timestamp.valueOf(timestamp);
        System.out.println("IMPORTANT -> Experience started at: " + timestamp);
        System.out.println("IMPORTANT -> Experience must end until: " + getExperienceLimitTimestamp());

        // For better data analysis
        try {
            csvFile = new File("inserts.csv");
            fw = new FileWriter(csvFile.getPath(), false);
            String header = "Insert,Topic,Time,Field1,Field2,isValid,Error\n";
            fw.append(header);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        experienceMustEnd = false;
    }

    public static void endExperience(Timestamp timestamp, String motive) {
        DBCollection exp = db.getCollection("exp");
        DBObject json = new BasicDBObject().append("StartTime", experienceBeginning)
                                            .append("EndTime", timestamp)
                                            .append("EndMotive", motive);
        exp.insert(json);
        experienceBeginning = null;
        isWaitingForExperienceStart = true;
        hasStartReadingArrived = false;
        experienceMustEnd = false;
        try {
            fw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DBObject getDBObjectFromReading (String reading) {
        try{
            DBObject document_json;
            document_json = (DBObject) JSON.parse(reading);
            return document_json;
        } catch (Exception e){
            System.out.println(e);
            return null;
        }

    }


    public static Timestamp getExperienceBeginning() { return experienceBeginning; }

    public static Timestamp getExperienceLimitTimestamp() {
        return new Timestamp(experienceBeginning.getTime() + TimeUnit.MINUTES.toMillis(10));
    }

    public static boolean isExperienceMustEnd() {
        return experienceMustEnd;
    }

    public static FileWriter getFileWriter() {return fw;}
}

