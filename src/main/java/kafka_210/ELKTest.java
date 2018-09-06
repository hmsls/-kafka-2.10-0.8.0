package kafka_210;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ELKTest {
	private static Producer<String,String> producer;
	private final Properties props ;
	
	public ELKTest(){
		props = new Properties();
		props.put("metadata.broker.list","0.0.0.0:9092");
		props.put("serializer.class","kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		producer = new Producer<String, String>(new ProducerConfig(props));
	}
	
	public static void main(String[] args) {
		ELKTest et = new ELKTest();
		String topic = "cluster-test";
		for(int i = 1;i<100;i++){
			String key = i + "";
			String value = "{"+"testbsb"+"tom"+i+"}";
			KeyedMessage<String, String> km = new KeyedMessage<String, String>(topic,key, value);
			producer.send(km);
		}
		producer.close();
		System.out.println("完成！");
	}
}
