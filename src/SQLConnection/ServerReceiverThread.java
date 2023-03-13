package SQLConnection;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

public class ServerReceiverThread extends Thread{
    private Socket socket;

    public ServerReceiverThread(Socket socket) {
        this.socket = socket;
    }

    public void run() {


        try {
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

            while (!socket.isClosed() && socket.isConnected()) {
                System.out.println("receiver waiting...");
                Message message = (Message) ois.readObject();
                System.out.println(message);
            }


        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Server couldn't receive or close channel on " + Thread.currentThread());
            e.printStackTrace();
        }

    }
}
