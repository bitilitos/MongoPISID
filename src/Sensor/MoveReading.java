package Sensor;

import java.io.Serializable;

public class MoveReading extends SensorReading implements Serializable {

    int inRoom;
    int outRoom;



    public MoveReading(String id, String timestampString, String inRoomString, String outRoomString) {
        super(id, timestampString);

        if ((this.inRoom = parseRoom(inRoomString)) == -1) {
            super.setReadingGood(false);
            super.setError(super.getError() + " InRoom wasn't parsable. ");
        }

        if ((this.outRoom = parseRoom(outRoomString)) == -1) {
            super.setReadingGood(false);
            super.setError(super.getError() + " outRoom wasn't parsable. ");
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

    @Override
    public String toString() {
        String result = "ReadingID: " + super.getId() + " ; Time: " + super.getTimestamp() + " ;  " +
                "InRoom: " + inRoom + "; OutRoom: " + outRoom + "; Valid Reading: " + super.isReadingGood();

        if (super.isReadingGood() == false) result += " ; Error: " + super.getError();
        return result;
    }



}
