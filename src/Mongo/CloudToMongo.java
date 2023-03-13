package Mongo;

import SQLConnection.Message;
import SQLConnection.MessageType;
import Sensor.MoveReading;
import Sensor.SensorReading;
import Sensor.TemperatureReading;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;
import org.eclipse.paho.client.mqttv3.*;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class CloudToMongo  extends Thread implements MqttCallback {

    MqttClient mqttclient;



    static BlockingQueue <Message> messagesToSendQueue = new LinkedBlockingQueue<Message>();

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

    public void run() {

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
        for (Map.Entry<String, String> topic : topics.entrySet()){
            Runnable thread = new Runnable() {
                @Override
                public void run() {
                    CloudToMongo cloudToMongo = new CloudToMongo();
                    cloudToMongo.connectCloud(topic.getKey());
                    cloudToMongo.connectMongo(topic.getValue());
                }
            };
            thread.run();
        }
    }

    private synchronized void insertToQueue (String id, String topic, DBObject json) {

        String sensorType = topics.get(topic);
        messagesToSendQueue.add(new Message(id, MessageType.SENSOR, sensorType, createSensorReadingObject(sensorType,json)));
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
        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURI));
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
            DBObject document_json;
            document_json = (DBObject) JSON.parse(c.toString());
            System.out.println(document_json);
            mongocol.insert(document_json);
            ObjectId id = (ObjectId) document_json.get("_id");
            insertToQueue(id.toString(), topic, document_json);
            //Apresentar Document na Window
            documentLabel.append(c.toString()+"\n");

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private SensorReading createSensorReadingObject (String sensorType, DBObject json) {

        SensorReading sensorReading;

        if (sensorType.equals("mov")) {
            sensorReading = new MoveReading(json.get("_id").toString(), json.get("Hora").toString(),
                    json.get("SalaEntrada").toString(), json.get("SalaSaida").toString());
        } else {
            sensorReading = new TemperatureReading(json.get("_id").toString(), json.get("Hora").toString(),
                    json.get("Leitura").toString(), json.get("Sensor").toString());
        }
        return sensorReading;


    }


    public static BlockingQueue<Message> getMessagesToSendQueue() {
        return messagesToSendQueue;
    }


    @Override
    public void connectionLost(Throwable cause) {
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {

    }
}

