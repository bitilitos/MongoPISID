package Sensor;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import netscape.javascript.JSObject;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;

public class TemperatureReading extends SensorReading {

    private static double standardDeviation = 1;
    private static double delta = 0;
    private static double variance = 0;
    private static double mean;
    private static int numberOfReadings;

    private static final int THRESHOLD = 1;
    private double readingValue;
    int sensorId;


    public TemperatureReading(String timestampString, String readingString, String sensorIdString) {
        super( timestampString);

        Double value;
        int sensor;
        Timestamp time;

        // Try to parse the Reading
        if ((value = parseReadingValue(readingString))==null) {
            super.setReadingGood(false);
            super.setError(super.getError() + "Reading value wasn't parsable. ");
            this.readingValue = -273.15;
        } else {
            readingValue = value;
            updateOutliersAlgorithm(readingValue);


            standardDeviation = getMean()*0.1;
        }

        // Try to parse the sensorId
        if ((sensor = parseSensorId(sensorIdString)) == -1) {
            super.setReadingGood(false);
            super.setError(super.getError() + "SensorId wasn't parsable. ");
        }
        sensorId = sensor;
    }

    private static void updateOutliersAlgorithm(double readingValue) {
        numberOfReadings++;
        updateDelta(readingValue);
        updateMean();
        updateVariance(readingValue);
        updateStandardDeviation();
    }

    private static void updateDelta (double readingValue) {mean = readingValue-getMean(); }
    private static void updateMean () { mean += (getMean() / numberOfReadings);}
    private static void updateVariance (double readingValue) {  variance += delta * (readingValue - mean);  }

    private static void updateStandardDeviation() { standardDeviation = Math.sqrt(variance/numberOfReadings);}

    private static double getZScore(Double readingValue) {
        return Math.abs(readingValue - mean)/(standardDeviation);
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

    public static double getMean() {
        return mean;
    }

    public double getReadingValue() {
        return readingValue;
    }

    public int getSensorId() {
        return sensorId;
    }

    public DBObject getDBObject() {
        return BasicDBObject.parse(this.getDocument().toJson());
    }

    public Document getDocument() {
        Document doc = new Document();
        doc.append("Hour", this.getTimestamp().toString());
        doc.append("Measure", this.readingValue);
        doc.append("Sensor", this.sensorId);
        doc.append("isValid", super.isReadingGood());
        doc.append("Error", super.getError());
        return doc;
    }



    @Override
    public String toString() {
        String result = "Time: " + super.getTimestamp() + " ,  " +
                "Temp: " + readingValue +   ",Sensor: " + sensorId + ", isValid: " + super.isReadingGood();

        if (super.isReadingGood() == false) result += " , Error: " + super.getError();
        return result;
    }


}
