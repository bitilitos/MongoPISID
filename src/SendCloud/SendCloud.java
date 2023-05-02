package SendCloud;

import Mongo.*;
import org.eclipse.paho.client.mqttv3.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class SendCloud  extends Thread implements MqttCallback  {
	static MqttClient mqttclient;
	static String cloud_server = new String();
    static String cloud_topic = new String();
	static String mongo_collections = new String();
	public BlockingQueue<String> data;

	public static void publishSensor(String leitura) {
		try {
			MqttMessage mqtt_message = new MqttMessage();
			mqtt_message.setPayload(leitura.getBytes());
			//Fazer um if aqui dentro para o cloud_topic.
			// se ele for do tópico das leituras, QoS1 e envia a mensagem assim
			//mqtt_message.publish(cloud_topic, mqtt_message, 1, false);
			// Se for do topico dos warning, faz QoS 2
			//mqtt_message.publish(cloud_topic, mqtt_message, 2, false);
			//para cada topico diferente, QOS diferente
			mqttclient.publish(cloud_topic, mqtt_message);
			System.out.println("Published a topic");
		} catch (MqttException e) {
			e.printStackTrace();}
	}

	public SendCloud(BlockingQueue<String> data, String cloud_topic) {
		this.data = data;
		this.cloud_topic = cloud_topic;
		connecCloud(cloud_topic);
	}

	@Override
	public void run() {
		while (CloudToMongo.getExperienceBeginning() != null) {
			if (mqttclient.isConnected()) {
				if (data.isEmpty()) {
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
			} else {
				System.out.println("MQTT on topic " + cloud_topic + " is not connected");

				try {
					sleep(1000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
//Retained Messages:
	//Brocker fica com a mensagem que foi pedida para reter. Só retem 1 mensagem por topico
	//Ideia: Primeira mensagem quando o cliente mongo se liga ao brocker, é retida e diz que o mongo está up.
	//através do lastwill, quando o mongo vai abaixo, é enviada uma mensagem a dizer que o mongo está down.
	//assim, no lado do mysql, conseguimos sempre saber que o mongo está up ou down através da mensagem retida no típico.
	//Eles tem a mensagem de inicio de experiencia tambem retida num tópico. Assim, o lado do mySql sabe sempre
	//que o script iniciou uma nova experiencia.

	public void connecCloud(String cloud_topic) {
        try {
			Properties p = new Properties();
			p.load(new FileInputStream("SendCloud.ini"));
			cloud_server = p.getProperty("cloud_server");
			this.cloud_topic = cloud_topic;
            mqttclient = new MqttClient(cloud_server, "SimulateSensor"+cloud_topic);
			MqttConnectOptions mqttconnectOptions = new MqttConnectOptions();
			//set user, set password setcleansessiontrue(a sessão quando é desconectada é limpa)
			//cleansession quando o cliente se desconecta, a sessão é limpa e não guarda os dados
			//mongo pode ser true e mysql false, mas vemos essa questão
			//lastWill
			//Definir um lightWarning como DBObject. preparar uma mensagem de erro. converter em string
			//através do mqttOptions setWill na mensagem. (setwill(topic, mensagem.getBytes, QOS,RETAINED)
			//lastwill é uma mensagem que é enviada quando o cliente se desconecta
			//o brocker so sabe passados 15s (default), podesse mudar atraves de setKeepAliveIntervale
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
