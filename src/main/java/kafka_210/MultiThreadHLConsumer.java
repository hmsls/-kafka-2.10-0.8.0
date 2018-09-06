package kafka_210;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class MultiThreadHLConsumer {
	private ExecutorService executor;
	private final ConsumerConnector consumer;
	private final String topic;
	public MultiThreadHLConsumer(String zookeeper,String groupId,String topic){
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "5000");
		props.put("zookeeper.sync.time.ms", "2500");
		props.put("auto.commit.interval.ms", "10000");
		
		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		this.topic = topic;
	}
	
	public void testConsumer(int threadCount){
		Map<String,Integer> topicCount = new HashMap<String,Integer>();
		// Define thread count for each topic
		topicCount.put(topic, new Integer(threadCount));
		// Here we have used a single topic but we can also add
		// multiple topics to topicCount MAP
		Map<String,List<KafkaStream<byte[],byte[]>>> consumerSteams = consumer.createMessageStreams(topicCount);
		List<KafkaStream<byte[],byte[]>> streams = consumerSteams.get(topic);
		
		// Launching the thread pool
		executor = Executors.newFixedThreadPool(threadCount);
		
		//Creating an object messages consumption
		int threadNumber = 0;
		for(final KafkaStream<byte[], byte[]> stream : streams){
			ConsumerIterator< byte[], byte[]> consumerIte = stream.iterator();
			threadNumber++;
			while(consumerIte.hasNext()){
				System.out.println("Message from thread :: " + threadNumber + " -- " + new String(consumerIte.next().message()));
			}
		}
		if(consumer != null){
			consumer.shutdown();
		}
		if(executor != null){
			executor.shutdown();
		}
	}
	public static void main(String[] args) {
		String topic = "kafkatopic";
		int threadCount = 3;
		MultiThreadHLConsumer mc = new MultiThreadHLConsumer("0.0.0.0::2181", "testgroup", topic);
		mc.testConsumer(threadCount);
	}
}
