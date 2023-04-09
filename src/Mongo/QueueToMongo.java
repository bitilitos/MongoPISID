package Mongo;


import Sensor.*;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueToMongo extends Thread{
    private BlockingQueue<String> readingsForMongo;
    private Map<Integer, TemperatureReading> lastTemperatureInMongo = new HashMap<>();
    private Map<Integer, TemperatureReading> lastTemperatureAlert = new HashMap<>();
    private Map<Integer, MutableInt> sensorFailureCount = new HashMap<>();
    private DBCollection mongocol;



     public QueueToMongo(DBCollection mongocol, BlockingQueue<String> readingsForMongo) {
         this.readingsForMongo = readingsForMongo;
         this.mongocol = mongocol;
    }


    public void run() {

        while (true) {
            //
            if (!readingsForMongo.isEmpty() && CloudToMongo.getExperienceBeginning() != null) {
                String reading = readingsForMongo.poll();
                String[] tempValues = parseSensorReadingToArray(reading);
                SensorReading sensorReading = null;

                if (mongocol.getName().equals("temp")) {
                    sensorReading = new TemperatureReading(tempValues[0], tempValues[1], tempValues[2]);
                    if (!checkIfTemperatureReadingIsToWrite(sensorReading))
                        continue;
                    else {
                        if (checkIfTemperatureReadingIsToAlert(sensorReading)){
                            CloudToMongo.insertAlert(new Alert(sensorReading, AlertType.Low, "Info: Temperature Variation bigger than 1 degree since last info. "));
                        }
                    }
                }
                else {
                    sensorReading = new MoveReading(tempValues[0], tempValues[1], tempValues[2]);
                }
                if (sensorReading != null) {
                    mongocol.insert(sensorReading.getDBObject());
                    String insert = "Mongo Insert,, " + sensorReading;
                    if (CloudToMongo.getFileWriter()!=null){
                        try {
                            CloudToMongo.getFileWriter().append(insert + "\n");
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    System.out.println(insert);
                }

            }
        }
    }



    // returns hora, leitura, sensor
    // returns hora, SalaEntrada, SalaSaida
    private String[] parseSensorReadingToArray(String reading) {
         String[] fields = reading.split(",");
         String[] values = new String[fields.length];
         int i = 0;

         for (String field : fields) {
             String tempRead[] = field.split(":", 2);
             values[i] = tempRead[1].trim();
             values[i] = values[i].replaceAll("\"","").trim();
             values[i] = values[i].replaceAll("}","").trim();
             i++;
         }

         return values;
    }

    private boolean checkIfTemperatureReadingIsToWrite(SensorReading sensorReading) {
         // if sensor has problem
         if (!sensorReading.isReadingGood()) {
             sensorFailureProcess(sensorReading);
             return true;
         }
        TemperatureReading temperatureReading = (TemperatureReading) sensorReading;
         //if no reading has been sent
         if (!lastTemperatureInMongo.containsKey(temperatureReading.getSensorId())) {
             lastTemperatureInMongo.put(temperatureReading.getSensorId(), temperatureReading);
             return true;
         }

         double lastTempValue = lastTemperatureInMongo.get(temperatureReading.getSensorId()).getReadingValue();
         double actualTempReading = temperatureReading.getReadingValue();
         BigDecimal lastTempValueBD = truncateDecimal(lastTempValue,2);
         BigDecimal actualTempReadingBD = truncateDecimal(actualTempReading, 2);

         // if reading is different
         if (!lastTempValueBD.equals(actualTempReadingBD)) {
             lastTemperatureInMongo.put(temperatureReading.getSensorId(), temperatureReading);
             return true;
         }

         //if reading is the same but last reading was over 5sec
         Timestamp lastTempTime = lastTemperatureInMongo.get(temperatureReading.getSensorId()).getTimestamp();
         if (temperatureReading.getTimestamp().after(new Timestamp(lastTempTime.getTime() + TimeUnit.SECONDS.toMillis(5)))){
             lastTemperatureInMongo.put(temperatureReading.getSensorId(), temperatureReading);
             return true;
         }

         return false;
    }


    private boolean checkIfTemperatureReadingIsToAlert(SensorReading sensorReading) {
        // if sensor has problem
        if (!sensorReading.isReadingGood()) return false;
        TemperatureReading temperatureReading = (TemperatureReading) sensorReading;
        //if no reading has been sent
        if (!lastTemperatureAlert.containsKey(temperatureReading.getSensorId())) {
            lastTemperatureAlert.put(temperatureReading.getSensorId(), temperatureReading);
            return true;
        }

        double lastTempAlert = lastTemperatureAlert.get(temperatureReading.getSensorId()).getReadingValue();
        double actualTempAlert = temperatureReading.getReadingValue();

        // if reading has 1 degree
        if (Math.abs(lastTempAlert-actualTempAlert) >= 1) {
            lastTemperatureAlert.put(temperatureReading.getSensorId(), temperatureReading);
            return true;
        }
        return false;
    }


    private static BigDecimal truncateDecimal(double x, int numberofDecimals)
    {
        if ( x > 0) {
            return new BigDecimal(String.valueOf(x)).setScale(numberofDecimals, BigDecimal.ROUND_FLOOR);
        } else {
            return new BigDecimal(String.valueOf(x)).setScale(numberofDecimals, BigDecimal.ROUND_CEILING);
        }
    }

    private void sensorFailureProcess(SensorReading sensorReading) {
         int key;
         if (mongocol.getName().equals("temp")) {
             TemperatureReading temperatureReading = (TemperatureReading) sensorReading;
             key = temperatureReading.getSensorId();
         }
         else {
             MoveReading moveReading = (MoveReading) sensorReading;
             key = Integer.parseInt((Integer.toString(moveReading.getEntranceRoom()) + Integer.toString(moveReading.getExitRoom())));
         }

         MutableInt count = sensorFailureCount.get(key);
         if (count==null){
             sensorFailureCount.put(key, new MutableInt());
         }
         else
             count.increment();

         // every 5 failures insert alert
         // first 5 medium alert, after high alert
         if (count!=null && (count.get() == 5 || count.get() == 50) ) {
             AlertType alertType;
             if (count.get() > 5)
                 alertType = AlertType.High;
             else
                 alertType = AlertType.Medium;

             Alert alert = new Alert(sensorReading, alertType, "Sensor with " + count.get() + " malfunctions! ");
             CloudToMongo.insertAlert(alert);
         }
    }

    static class MutableInt {
        int value = 1; // note that we start at 1 since we're counting
        public void increment () { ++value;      }
        public int  get ()       { return value; }
    }

}
