package Mongo;

import SQLConnection.Message;
import Sensor.SensorReading;
import Sensor.TemperatureReading;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class QueueToSend extends Thread{

    private BlockingQueue<Message> messagesToSendQueue;
    private Map<Integer, TemperatureReading> lastTemperaturesSent = new HashMap<>();

    private BlockingQueue<Message> messagesWaitingForConfirmation = new LinkedBlockingQueue<>();

    private Socket socket;
     public QueueToSend(BlockingQueue messagesToSendQueue, Socket socket) {
        this.messagesToSendQueue = messagesToSendQueue;
        this.socket = socket;
    }


    public void run() {

        while (true) {
            if (!messagesToSendQueue.isEmpty()) {
                Message message = messagesToSendQueue.poll();
                // sendMessage(socket, message);

                if (message.getSensorReading() != null ) {
                    SensorReading sensorReading = message.getSensorReading();
                    if (sensorReading instanceof TemperatureReading) {
                        TemperatureReading temperatureReading = (TemperatureReading) sensorReading;
                        if (checkIfTemperatureReadingIsToSend(temperatureReading)){
                            sendMessage(socket, message);
                            lastTemperaturesSent.put(temperatureReading.getSensorId(), temperatureReading);
                        }
                        //If not temperatureReading
                    } else
                        sendMessage(socket, message);
                }


            }
        }
    }

    private boolean checkIfTemperatureReadingIsToSend(TemperatureReading temperatureReading) {
         //if no reading has been sent
         if (!lastTemperaturesSent.containsKey(temperatureReading.getSensorId())) return true;

         double lastTempValue = lastTemperaturesSent.get(temperatureReading.getSensorId()).getReadingValue();
         // if reading is different
         if (lastTempValue != temperatureReading.getReadingValue()) return true;

         //if reading is the same but last reading was over 5sec
         Timestamp lastTempTime = lastTemperaturesSent.get(temperatureReading.getSensorId()).getTimestamp();
         if (lastTempTime.compareTo(temperatureReading.getTimestamp()) > 5000) return true;
         return false;
    }

    private static ObjectOutputStream oos;

    public static void sendMessage(Socket socket, Message message) {

        try {
            if (oos == null) oos = new ObjectOutputStream(socket.getOutputStream());

            oos.writeObject(message);
            oos.flush();

            System.out.println("Message " + message + "\n was sent!");
        } catch (IOException e) {
            System.out.println("Message " + message + "\n cloudn't be sent!");
            e.printStackTrace();
        }
    }

    public BlockingQueue<Message> getMessagesWaitingForConfirmation() {
        return messagesWaitingForConfirmation;
    }
}
