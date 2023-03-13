package Mongo;

import SQLConnection.Message;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;


public class QueueToSend extends Thread{

    private BlockingQueue<Message> messagesToSendQueue;
    private Map<String, String> lastTemperatureSent = new HashMap<>();

    private Socket socket;
     public QueueToSend(BlockingQueue messagesToSendQueue, Socket socket) {
        this.messagesToSendQueue = messagesToSendQueue;
        this.socket = socket;
    }


    public void run() {

        while (true) {
            if (!messagesToSendQueue.isEmpty()) {
                Message message = messagesToSendQueue.poll();
                sendMessage(socket, message);
            }
        }
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
    private void printJSon(String JSon) {
        //JSONParser parser = new JSONParser();
    }

    private boolean updateTemperature (String json) {
        return true;
    }

}
