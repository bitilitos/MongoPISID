package Sensor;

import Mongo.CloudToMongo;

import com.mongodb.DBObject;
import java.sql.Timestamp;

public abstract class SensorReading {


    private boolean readingGood = true;

    private String error = "";

    private Timestamp timestamp;
    private static Timestamp lastTimeStamp;

    public SensorReading(String timestampString ) {

        Timestamp time;
        time = parseTimestamp(timestampString);
        if (time == null) {
            this.timestamp = new Timestamp(0,0,0,0,0,0,0);
        }
        else if (CloudToMongo.getExperienceBeginning() != null) {
                if (timestampString.equals("2000-01-01 00:00:00") && !CloudToMongo.isExperienceMustEnd()) {
                    if (this instanceof MoveReading) {
                        MoveReading moveReading = (MoveReading) this;
                        if (moveReading.entranceRoom == 0 && moveReading.exitRoom == 0) {
                            timestamp = time;
                        }
                    }
                }

                else if (timestampString.equals("2000-01-01 00:00:00")) {
                    if (this instanceof MoveReading) {
                        MoveReading moveReading = (MoveReading) this;
                        if (moveReading.entranceRoom == 0 && moveReading.exitRoom == 0 && lastTimeStamp != null) {
                            timestamp = lastTimeStamp;
                            CloudToMongo.endExperience(lastTimeStamp, "A new experience has started. ");
                        }
                    }
                }
                else if (time.before(CloudToMongo.getExperienceBeginning())) {
                    this.setReadingGood(false);
                    this.setError("This reading is from a past experience. ");
                    this.timestamp = time;

                } else if (time.after(CloudToMongo.getExperienceLimitTimestamp()))  { //10 Minutes
                    this.setReadingGood(false);
                    this.setError("This reading will be from a future experience. ");
                    this.timestamp = time;
                }
                else {
                    this.timestamp = time;
                    lastTimeStamp = time;
                }
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


    public abstract DBObject getDBObject();

    public static Timestamp getLastTimeStamp() {
        return lastTimeStamp;
    }
}
