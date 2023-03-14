package Sensor;

import com.mongodb.DBCollection;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;
import java.sql.Timestamp;

public class SensorReading implements Serializable {


    private boolean readingGood = true;

    private String error = "";

    private Timestamp timestamp;


    private DBCollection mongoCol;

    public SensorReading(DBCollection mongoCol, String timestampString ) {
        this.mongoCol = mongoCol;
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



    public Timestamp getTimestamp() {
        return timestamp;
    }

    public DBCollection getMongoCol() {
        return mongoCol;
    }


}
