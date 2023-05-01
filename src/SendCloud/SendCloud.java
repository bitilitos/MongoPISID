package SendCloud;

import Mongo.*;
import org.eclipse.paho.client.mqttv3.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class SendCloud  extends Thread implements MqttCallback  {
	private static MqttClient mqttclient;
	private static String cloud_server = new String();
    private String cloud_topic = new String();
	private String mongo_collections = new String();

	public BlockingQueue<String> data;

	public void publishSensor(String leitura) {
		try {
			MqttMessage mqtt_message = new MqttMessage();
			mqtt_message.setPayload(leitura.getBytes());
			mqttclient.publish(cloud_topic, mqtt_message);
			System.out.println("Published topic " + cloud_topic + " : message:" + mqtt_message);
		} catch (MqttException e) {
			e.printStackTrace();}
	}



	public SendCloud(BlockingQueue<String> data, String cloud_topic) {
		this.data = data;
		this.cloud_topic = cloud_topic;
		connectCloud(cloud_topic);
	}

	@Override
	public void run() {
		while (CloudToMongo.getExperienceBeginning() != null) {
			if (mqttclient.isConnected()) {
				if(data.isEmpty()) {
					System.out.println("MQTT on topic " + cloud_topic + " has no data");
					try {
						sleep(1000);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
					continue;
				}
				try {
					String leitura = data.take();
					System.out.println("Pub dataStrut: " + data.hashCode() + " Topic " + cloud_topic);
					publishSensor(leitura);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else {
				System.out.println("MQTT on topic " + cloud_topic + " is not connected");

				try {
					sleep(1000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

		}
	}

	public void connectCloud(String cloud_topic) {
        try {
			Properties p = new Properties();
			p.load(new FileInputStream("SendCloud.ini"));
			cloud_server = p.getProperty("cloud_server");
			this.cloud_topic = cloud_topic;
            mqttclient = new MqttClient(cloud_server, "SimulateSensor" + cloud_topic);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic);
			System.out.println("Cloud connected is " + mqttclient.isConnected() + " on topic " + cloud_topic);
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
		System.out.println("Delivery complete\n" );
		System.out.println("Message sent: " + token.getMessageId() + "\n");
	}

	@Override
	public void messageArrived(String topic, MqttMessage message){ }

}
