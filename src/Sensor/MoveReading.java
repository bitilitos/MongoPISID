package Sensor;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import netscape.javascript.JSObject;
import org.bson.Document;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;

public class MoveReading extends SensorReading implements Serializable {

    @BsonProperty(value = "SalaEntrada")
    int inRoom;
    @BsonProperty (value = "SalaSaida")
    int outRoom;



    public MoveReading(DBCollection mongoCol, String timestampString, String inRoomString, String outRoomString) {
        super(mongoCol, timestampString);

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

    public DBObject getDBObject() {
        Document doc = new Document();
        doc.append("Hora", this.getTimestamp().toString());
        doc.append("SalaEntrada", this.inRoom);
        doc.append("SalaSaida", this.outRoom);
        doc.append("LeituraCorrecta", super.isReadingGood());
        doc.append("Erro", super.getError());
        return BasicDBObject.parse(doc.toJson());

    }

    @Override
    public String toString() {
        String result = "Time: " + super.getTimestamp() + " ;  " +
                "InRoom: " + inRoom + "; OutRoom: " + outRoom + "; Valid Reading: " + super.isReadingGood();

        if (super.isReadingGood() == false) result += " ; Error: " + super.getError();
        return result;
    }



}
