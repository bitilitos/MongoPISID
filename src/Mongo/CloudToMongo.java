package Mongo;

import Sensor.Alert;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.eclipse.paho.client.mqttv3.*;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;




public class CloudToMongo implements MqttCallback {

    public static final String BACKUP_JAR_PATH = "/home/bitos/IdeaProjects/BackupMongoPISID/out/artifacts/BackupMongoPISID_jar/BackupMongoPISID.jar";
    private MqttClient mqttclient;

    private static MongoClient mongoClient;

    private static Mongo mongo;


    private static DB db;
    private DBCollection mongocol;
    public static DBCollection alertCollection;
    private static String mongo_user = new String();
    private static String mongo_password = new String();
    private static String mongo_address = new String();
    private static String cloud_server = new String();
    private String cloud_topic = new String();
    private static String mongo_host = new String();
    private static String mongo_replica = new String();
    private static String mongo_database = new String();
    private static String mongo_collection = new String();
    public static String mongo_authentication = new String();
    private static JTextArea documentLabel = new JTextArea("\n");
    private static Map<String, String> topics = new HashMap<>();
    private BlockingQueue <String> readingsForMongo = new LinkedBlockingQueue<>();


    // Flag -> Waiting for Experience Reading to arrive
    private static boolean isWaitingForExperienceStart = true;
    // Flag -> Message indicating experience start has arrived
    private static boolean hasStartReadingArrived = false;

    private static boolean experienceMustEnd = false;


    private static Timestamp experienceBeginning = null;

    private static File csvFile = new File("inserts.csv");
    private static FileWriter fw;

    private static final String CLOUD_TO_MONGO_INI_PATH = "/home/bitos/IdeaProjects/MongoPISID/CloudToMongo.ini";
    private static final String BACKUP_AUTOMATIC_RUN = "BACKUP_AUTOMATIC_RUN";

    private static boolean runFromBackup = false;
    private static List<CloudToMongo> cloudToMongoList = new ArrayList<CloudToMongo>();

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
            p.load(new FileInputStream(CLOUD_TO_MONGO_INI_PATH));
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
                    cloudToMongoList.add(cloudToMongo);
                }
            };
            thread.run();
        }
        alertCollection = db.getCollection("alert");

    }

    private static void startQueueToMongo() {

        for (CloudToMongo cloudToMongo : cloudToMongoList) {
            QueueToMongo queueToMongo = new QueueToMongo(cloudToMongo.mongocol, cloudToMongo.readingsForMongo);
            queueToMongo.start();
        }

    }

    private void insertToQueue (String topic, String reading) {
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
            // insertToQueue(topic, c.toString());
            DBObject json = getDBObjectFromReading(c.toString());

                // To get Start Message when expecting experience to Start
                if (isWaitingForExperienceStart) {
                    if (json != null) {
                        if (isReadingExperienceStart(json)) insertToQueue(topic, c.toString());
                        return;
                    }
                }


                // To get first timeStamp after start experience message
                if (hasStartReadingArrived && experienceBeginning == null) {
                    if (json != null) {
                        String timestamp = json.get("Hora").toString();
                        if (timestamp != null && !timestamp.isEmpty()) startExperience(timestamp);
                        insertToQueue(topic, c.toString());
                    }
                }

                // When experience is running to get start of new experience
                else if (experienceBeginning != null)  {
                    if (json != null) {
                        isReadingExperienceStart(json);
                    }
                    insertToQueue(topic, c.toString());
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
        System.out.println("Connection Lost with Cloud!!!");
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {

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
        startQueueToMongo();

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
        cleanDataReadingsForMongo();

        try {
            fw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void cleanDataReadingsForMongo() {
        for (CloudToMongo cloudToMongo : cloudToMongoList) {
            cloudToMongo.readingsForMongo.clear();
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

    public static void insertAlert(Alert alert) {
        CloudToMongo.getAlertCollection().insert(alert.getDBObject());
        System.out.println("Alert Insert, " + alert);
        try {
            CloudToMongo.getFileWriter().append("Alert Insert, ,").append(String.valueOf(alert)).append("\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isMongoConnected() {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        // builder settings
        ServerAddress ServerAddress = new ServerAddress("localhost", 25019);
        MongoClient mongoClient = new MongoClient(ServerAddress, builder.build());

        try {
            mongoClient.getConnectPoint();
            return true;
        } catch (Exception e) {
            System.out.println("MongoDB Server is Down");
            return false;
        } finally {
            mongoClient.close();
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

    public static DB getDb() {
        return db;
    }

    public static DBCollection getAlertCollection() {return alertCollection;}

    private static void runProcess(String command) throws Exception {
        Process pro = Runtime.getRuntime().exec(command);
        printLines(command + " stdout:", pro.getInputStream());
        printLines(command + " stderr:", pro.getErrorStream());
        pro.waitFor();
        System.out.println(command + " exitValue() " + pro.exitValue());
    }

    private static void printLines(String cmd, InputStream ins) throws Exception {
        String line = null;
        BufferedReader in = new BufferedReader(
                new InputStreamReader(ins));
        while ((line = in.readLine()) != null) {
            System.out.println(cmd + " " + line);
        }
    }

    private static void manualRun() {
        CloudToMongo.initiate();
        Runnable backup = new Runnable() {
            @Override
            public void run() {
                try {
                    runProcess("java -jar " + CloudToMongo.BACKUP_JAR_PATH + " " +ProcessHandle.current().pid());
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        };
        backup.run();

    }

    private static void backupAutomaticRun() {
        runFromBackup = true;
        System.out.println(BACKUP_AUTOMATIC_RUN);
        CloudToMongo.initiate();

    }



    public static void main(String[] args) {
        if (args.length != 0 && args[0] != null && args[0].equals(BACKUP_AUTOMATIC_RUN))
            backupAutomaticRun();

        else manualRun();
    }


}

