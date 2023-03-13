package Sensor;

import java.io.Serializable;
import java.sql.Timestamp;

public class SensorReading implements Serializable {

    private boolean readingGood = true;
    private String error = "";
    private String id;
    private Timestamp timestamp;

    public SensorReading(String id, String timestampString ) {
        this.id = id;
        Timestamp time;

        if ((time = parseTimestamp(timestampString)) == null) {
            this.timestamp = new Timestamp(0,0,0,0,0,0,0);
        } else {
            this.timestamp = time;
        }

    }

    private Timestamp parseTimestamp (String timestampString) {
        Timestamp result;
        try {
            result = Timestamp.valueOf(timestampString);

        }catch (IllegalArgumentException e) {
            return null;
        }
        return result;
    }

    public boolean isReadingGood() {
        return readingGood;
    }

    public void setReadingGood(boolean readingGood) {
        this.readingGood = readingGood;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getId() {
        return id;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }
}
