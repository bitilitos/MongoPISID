package SendCloud;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;



import java.util.*;

import java.io.*;
import javax.swing.*;
import java.util.List;

public class SendCloud  implements MqttCallback  {
	static MqttClient mqttclient;
	static String cloud_server = new String();
    static String cloud_topic = new String();
	static String mongo_collections = new String();
	static JTextArea textArea = new JTextArea(10, 50);
	static Map<String, String> collectionsToTablesMap = new HashMap<>();

	public static void publishSensor(String leitura) {
		try {
			MqttMessage mqtt_message = new MqttMessage();
			mqtt_message.setPayload(leitura.getBytes());
			mqttclient.publish(cloud_topic, mqtt_message);
		} catch (MqttException e) {
			e.printStackTrace();}
	}

	public void readData(List<String> data){
		for (String s : data) {
			publishSensor(s);
		}
	}

	public void connecCloud(String cloud_topic) {
        try {
			Properties p = new Properties();
			p.load(new FileInputStream("SendCloud.ini"));
			cloud_server = p.getProperty("cloud_server");
			this.cloud_topic = cloud_topic;
            mqttclient = new MqttClient(cloud_server, "SimulateSensor"+cloud_topic);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic);
        } catch (MqttException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	@Override
	public void connectionLost(Throwable cause) {
		System.out.println("Connection lost");
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		System.out.println("Delivery complete");
		System.out.println("Message sent: " + token.getMessageId());
	}

	@Override
	public void messageArrived(String topic, MqttMessage message){ }

}
