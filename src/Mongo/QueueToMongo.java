package Mongo;


import Sensor.MoveReading;
import Sensor.SensorReading;
import Sensor.TemperatureReading;
import com.mongodb.DBCollection;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueToMongo extends Thread{
    private BlockingQueue<String> readingsForMongo;
    private Map<Integer, TemperatureReading> lastTemperatureInMongo = new HashMap<>();
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

                    if (checkIfTemperatureReadingIsToWrite(new TemperatureReading(tempValues[0], tempValues[1], tempValues[2]))){
                        sensorReading = new TemperatureReading(tempValues[0], tempValues[1], tempValues[2]);
                    }


                }
                else {
                    sensorReading = new MoveReading(tempValues[0], tempValues[1], tempValues[2]);
                }
                if (sensorReading != null) {
                    mongocol.insert(sensorReading.getDBObject());
                    System.out.println("Mongo Insert,, " + sensorReading);
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

    private boolean checkIfTemperatureReadingIsToWrite(TemperatureReading temperatureReading) {
         if (!temperatureReading.isReadingGood()) return true;
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

    private static BigDecimal truncateDecimal(double x, int numberofDecimals)
    {
        if ( x > 0) {
            return new BigDecimal(String.valueOf(x)).setScale(numberofDecimals, BigDecimal.ROUND_FLOOR);
        } else {
            return new BigDecimal(String.valueOf(x)).setScale(numberofDecimals, BigDecimal.ROUND_CEILING);
        }
    }


}
