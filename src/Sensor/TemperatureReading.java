package Sensor;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.Document;

import java.sql.Timestamp;

public class TemperatureReading extends SensorReading {

    private static double standardDeviation = 1;
    private static double delta = 0;
    private static double variance = 0;
    private static double mean;
    private static int numberOfReadings;

    private static final int THRESHOLD = 3;

    private double readingMean = 0;

    private double zScore = 0;
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
            this.readingValue = -99.99;
        } else {
            readingValue = value;
            updateOutliersAlgorithm(readingValue);
            this.readingMean = mean;
            if ((zScore = getZScore(readingValue)) > THRESHOLD) {
                super.setReadingGood(false);
                super.setError(super.getError() + "Temperature Reading is an outlier ");
            }
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

    private static void updateDelta (double readingValue) {delta = readingValue-getMean(); }
    private static void updateMean () { mean += (delta / numberOfReadings);}
    private static void updateVariance (double readingValue) {  variance += delta * (readingValue - mean);  }

    private static void updateStandardDeviation() { standardDeviation = Math.sqrt(variance/numberOfReadings);}

    private static double getZScore(Double readingValue) {
        double z = Math.abs(readingValue - mean)/(standardDeviation);
        System.out.println("Z_SCORE = " + z);
        return z;

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

    public double getReadingMean() {
        return readingMean;
    }

    public double getzScore() {
        return zScore;
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
                "Temp: " + readingValue +   ",Sensor: " + sensorId + ", isValid: " + super.isReadingGood() +
                " , Mean: " + mean + ", Z-Score: " + zScore + ", ";

        if (super.isReadingGood() == false)
            result += " , Error: " + super.getError();



        return result;
    }


}
