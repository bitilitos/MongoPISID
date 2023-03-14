package Mongo;

import Sensor.MoveReading;
import Sensor.SensorReading;
import Sensor.TemperatureReading;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
public class QueueToMongo extends Thread{

    private BlockingQueue<SensorReading> readingsForMongo;
    private Map<Integer, TemperatureReading> lastTemperatureInMongo = new HashMap<>();
    MongoCollection<SensorReading> collection;

     public QueueToMongo(BlockingQueue readingsForMongo) {
        this.readingsForMongo = readingsForMongo;
    }


    public void run() {

        while (true) {
            if (!readingsForMongo.isEmpty()) {
                SensorReading  sensorReading= readingsForMongo.poll();
                if (sensorReading != null) {
                    if (sensorReading instanceof TemperatureReading) {
                        TemperatureReading temperatureReading = (TemperatureReading) sensorReading;
                        if (checkIfTemperatureReadingIsToWrite(temperatureReading)) {

                           temperatureReading.getMongoCol().insert(temperatureReading.getDBObject());
                        }
                    }
                    if (sensorReading instanceof MoveReading) {
                            MoveReading moveReading = (MoveReading) sensorReading;
                            moveReading.getMongoCol().insert(moveReading.getDBObject());
                    }


                }
            }
        }
    }

    private boolean checkIfTemperatureReadingIsToWrite(TemperatureReading temperatureReading) {
         //if no reading has been sent
         if (!lastTemperatureInMongo.containsKey(temperatureReading.getSensorId())) return true;

         double lastTempValue = lastTemperatureInMongo.get(temperatureReading.getSensorId()).getReadingValue();
         // if reading is different
         if (lastTempValue != temperatureReading.getReadingValue()) return true;

         //if reading is the same but last reading was over 5sec
         Timestamp lastTempTime = lastTemperatureInMongo.get(temperatureReading.getSensorId()).getTimestamp();
         if (lastTempTime.compareTo(temperatureReading.getTimestamp()) > 5000) return true;
         return false;
    }




}
