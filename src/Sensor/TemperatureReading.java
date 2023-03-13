package Sensor;

import java.io.Serializable;
import java.sql.Timestamp;

public class TemperatureReading extends SensorReading implements Serializable {

    double readingValue;
    int sensorId;


    public TemperatureReading(String id, String timestampString, String readingString, String sensorIdString) {
        super(id, timestampString);

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

    @Override
    public String toString() {
        String result = "ReadingID: " + super.getId() + " ; Time: " + super.getTimestamp() + " ;  " +
                "Sensor: " + sensorId + "; Temp: " + readingValue + "; Valid Reading: " + super.isReadingGood();

        if (super.isReadingGood() == false) result += " ; Error: " + super.getError();
        return result;
    }


}
