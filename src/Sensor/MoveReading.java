package Sensor;

import Mongo.CloudToMongo;
import com.mongodb.BasicDBObject;

import com.mongodb.DBObject;
import org.bson.Document;


public class MoveReading extends SensorReading {


    int entranceRoom;

    int exitRoom;




    public MoveReading(String timestampString, String entranceRoomString, String exitRoomString) {
        super(timestampString);

        if ((this.entranceRoom = parseRoom(entranceRoomString)) == -1) {
            super.setReadingGood(false);
            super.setError(super.getError() + " EntranceRoom wasn't parsable. ");
        }

        if ((this.exitRoom = parseRoom(exitRoomString)) == -1) {
            super.setReadingGood(false);
            super.setError(super.getError() + " ExitRoom wasn't parsable. ");
        }

        if((this.entranceRoom == this.exitRoom)) {
            super.setReadingGood(false);
            if (this.entranceRoom == 0 && timestampString.equals("2000-01-01 00:00:00")) {
                if (!CloudToMongo.isExperienceMustEnd())
                    super.setError(super.getError() + "INFO:0 - Experience Start Message. ");
                else {
                    CloudToMongo.endExperience(SensorReading.getLastTimeStamp(),"New experience started.");
                    super.setError(super.getError() + "INFO:1 - New Experience Start Message while experience running, +" +
                            "stopped collecting data. ");
                }

            }
            else
                super.setError(super.getError() + " Corridor can't start and end in the same room. ");

        }

    }

    private int parseRoom (String roomString) {
        int result;
        try{
            result = Integer.parseInt(roomString);
        }catch (NumberFormatException | NullPointerException e) {
            return  -1;
        }

        return result;
    }

    public DBObject getDBObject() {
        Document doc = new Document();
        doc.append("Hour", this.getTimestamp().toString());
        doc.append("EntranceRoom", this.entranceRoom);
        doc.append("ExitRoom", this.exitRoom);
        doc.append("isValid", super.isReadingGood());
        doc.append("Error", super.getError());
        return BasicDBObject.parse(doc.toJson());
    }

    @Override
    public String toString() {
        String result = "Time: " + super.getTimestamp() + " ,  " +
                "EntranceRoom: " + entranceRoom + ", ExitRoom: " + exitRoom + ", isValid: " + super.isReadingGood();

        if (super.isReadingGood() == false) result += " , Error: " + super.getError();
        return result;
    }





}
