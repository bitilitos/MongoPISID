package SQLConnection;


import Sensor.MoveReading;
import Sensor.SensorReading;
import com.mongodb.DBObject;

import java.io.Serializable;


public class Message implements Serializable {

    String id;
    MessageType messageType;
    SensorReading sensorReading;

    String content;

    String sensorType;

    public Message (String id, MessageType messageType, String sensorType, SensorReading sensorReading){
        this.id = id;
        this.messageType = messageType;
        this.sensorReading = sensorReading;
        this.sensorType = sensorType;
    }

    public Message (String id, MessageType messageType,  String content) {
        this.id = id;
        this.messageType = messageType;
        this.content = content;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString () {
        String result = "Message ID: " + id + "\n" + "Type: " + messageType + "\n";
        if (messageType == MessageType.SENSOR) {
            result += "SensorType: " + sensorType + "\n";
            if (sensorType.equals("mov"))
                result += sensorReading.toString();
        }
        else { result  +=    "Content: " + getContent(); }
        return result;
    }



}


