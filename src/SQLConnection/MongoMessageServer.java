package SQLConnection;

import Mongo.CloudToMongo;
import Mongo.QueueToSend;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class MongoMessageServer extends Thread{

    ServerSocket serverSocket;
    private InetAddress address;
    private static BlockingQueue<Socket> clientSockets = new LinkedBlockingQueue<Socket>();


    @Override
    public void run() {

        try {
            serverSocket = new ServerSocket(8888);
            address = InetAddress.getLocalHost();


            while (true) {
                waitingConnections();
            }
        } catch (IOException e) {
            System.out.println("Unable to establish connection: " + e);
        }

    }

    private void waitingConnections() {

        try {

            System.out.println("Waiting for clients to connect on " +
                    InetAddress.getLocalHost().getHostName() + "...");

            // connect it to client socket
            Socket socket = serverSocket.accept();
            if (clientSockets.isEmpty()) startCollecting();
            clientSockets.add(socket);
            sendDataToSocket(socket);
            ServerReceiverThread serverThread = new ServerReceiverThread(socket);
            serverThread.start();


        } catch (IOException e) {
            System.out.println("Unable to establish connection: " + e);
        }
    }

    private void startCollecting(){
        CloudToMongo cloudToMongo = new CloudToMongo();
        cloudToMongo.start();

    }

    private void sendDataToSocket(Socket socket) {
        QueueToSend queueToMongo = new QueueToSend(CloudToMongo.getMessagesToSendQueue(), socket);
        queueToMongo.start();
    }

    public static BlockingQueue<Socket> getClientSockets() { return clientSockets; }


    public static void main(String[] args) {

        MongoMessageServer mongoMessageServer = new MongoMessageServer();
        mongoMessageServer.start();
    }


}

