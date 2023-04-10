package Sensor;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.Document;

import java.sql.Timestamp;

public class Alert {

    Timestamp timestamp = null;
    SensorReading sensorReading = null;
    AlertType alertType;
    String message;

    public Alert(AlertType alertType, String message){
        this.timestamp = new Timestamp(System.currentTimeMillis());
        this.alertType = alertType;
        this.message = message;
    }

    public Alert(SensorReading sensorReading, AlertType alertType, String message){
        this.sensorReading = sensorReading;
        this.alertType = alertType;
        this.message = message;
    }


    public Timestamp getTimestamp() {
        return timestamp;
    }

    public SensorReading getSensorReading() {
        return sensorReading;
    }

    public AlertType getAlertType() {
        return alertType;
    }

    public String getMessage() {
        return message;
    }

    public BasicDBObject getDBObject() {
        if (sensorReading != null) {
            Document doc = (sensorReading.getDocument());
            doc.append("AlertType", this.alertType.name());
            doc.append("Message", this.message);
            return BasicDBObject.parse(doc.toJson());
        } else {
            Document doc = new Document();
            doc.append("Hour", this.getTimestamp().toString());
            doc.append("AlertType", this.alertType.name());
            doc.append("Message", this.message);
            return BasicDBObject.parse(doc.toJson());
        }
    }

    @Override
    public String toString() {
        String result = "";

        if (sensorReading != null) {
            result = sensorReading + " ,AlertType:" + alertType + " ,Message:" + message;
        }
        else {
            result = "Hour:" + timestamp + " ,AlertType:" + alertType + " ,Message:" + message;
        }
        return result;
    }
}
