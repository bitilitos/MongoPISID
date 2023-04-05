package Mongo;


import Sensor.MoveReading;
import Sensor.SensorReading;
import Sensor.TemperatureReading;
import com.mongodb.DBCollection;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

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
                    sensorReading = new TemperatureReading(tempValues[0], tempValues[1], tempValues[2]);
                }
                else {
                    sensorReading = new MoveReading(tempValues[0], tempValues[1], tempValues[2]);
                }
                mongocol.insert(sensorReading.getDBObject());
                System.out.println("Mongo Insert: " + sensorReading);
            }
        }
    }


    private boolean isReadingExperienceStart(String[] tempValues) {
         return (tempValues[0].equals("2000-01-01 00:00:00") && tempValues[1].equals("0") && tempValues[2].equals("0"));
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
