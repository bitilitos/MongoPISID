package Sensor;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import netscape.javascript.JSObject;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;
import java.sql.Timestamp;

public class TemperatureReading extends SensorReading implements Serializable {

    @BsonProperty(value = "leitura")
    double readingValue;

    @BsonProperty(value = "_id")
    int sensorId;




    public TemperatureReading(DBCollection mongoCol, String timestampString, String readingString, String sensorIdString) {
        super(mongoCol, timestampString);

        Double value;
        int sensor;
        Timestamp time;

        // Try to parse the Reading
        if ((value = parseReadingValue(readingString))==null) {
            super.setReadingGood(false);
            super.setError(super.getError() + "Reading value wasn't parsable. ");
            this.readingValue = -273.15;
        } else {readingValue = value; }

        // Try to parse the sensorId
        if ((sensor = parseSensorId(sensorIdString)) == -1) {
            super.setReadingGood(false);
            super.setError(super.getError() + "SensorId wasn't parsable. ");
        }
        sensorId = sensor;
    }

    private Double parseReadingValue (String readingValue) {
        double result;
        try{
            result = Double.parseDouble(readingValue);
        }catch (NumberFormatException | NullPointerException e) {
            return  null;
        }

        return result;
    }

    private int parseSensorId (String sensorIdString) {
        int result;
        try{
            result = Integer.parseInt(sensorIdString);
        }catch (NumberFormatException | NullPointerException e) {
            return  -1;
        }

        return result;
    }

    public double getReadingValue() {
        return readingValue;
    }

    public int getSensorId() {
        return sensorId;
    }

    public DBObject getDBObject() {
        Document doc = new Document();
        doc.append("Hour", this.getTimestamp().toString());
        doc.append("Measure", this.readingValue);
        doc.append("Sensor", this.sensorId);
        doc.append("isValid", super.isReadingGood());
        doc.append("Error", super.getError());
        return BasicDBObject.parse(doc.toJson());

    }



    @Override
    public String toString() {
        String result = "Time: " + super.getTimestamp() + " ;  " +
                "Sensor: " + sensorId + "; Temp: " + readingValue + "; Valid Reading: " + super.isReadingGood();

        if (super.isReadingGood() == false) result += " ; Error: " + super.getError();
        return result;
    }


}
