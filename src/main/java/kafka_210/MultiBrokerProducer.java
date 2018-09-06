package kafka_210;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MultiBrokerProducer {
	private static Producer<Integer,String> producer;
	private final Properties props = new Properties();
	
	public MultiBrokerProducer(){
		Properties props = new Properties();
		props.put("metadata.broker.list","0.0.0.0:9092");
		props.put("serializer.class","kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		props.put("partitioner.class", "kafka_210.SimplePartitioner");
		producer = new Producer<Integer, String>(new ProducerConfig(props));
	}
	
	public static void main(String[] args) {
		MultiBrokerProducer mp = new MultiBrokerProducer();
		Random rnd = new Random();
		String topic = (String)args[0];
		for(int numCount = 0 ; numCount < 10 ; numCount ++){
			Integer key = rnd.nextInt(255);
			String msg = "This message for key - " + key ;
			KeyedMessage<Integer, String> data1 = new KeyedMessage<Integer, String>(topic, key, msg);
			producer.send(data1);
		}
		producer.close();
	}
}
