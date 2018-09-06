package kafka_210;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	private static Producer<Integer,String> producer;
	private final Properties props = new Properties();
	
	public KafkaProducer(){
		Properties props = new Properties();
//		props.put("metadata.broker.list","bigdata01:9092");
		props.put("metadata.broker.list","0.0.0.0:9092");
		props.put("serializer.class","kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		producer = new Producer<Integer, String>(new ProducerConfig(props));
	}
	public static void main(String[] args) {
		KafkaProducer kp = new KafkaProducer();
		String topic = (String)args[0];
		String message = (String)args[1];
		KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(topic, message);
		producer.send(data);
		producer.close();
		System.out.println("发送消息成功");
	}
}
