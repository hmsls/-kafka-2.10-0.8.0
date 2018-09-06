package kafka_210;

import java.util.Map;

import kafka.cluster.Cluster;
import kafka.producer.Partitioner;

public class SimplePartitioner implements Partitioner<Integer> {
	public void configure(Map<String, ?> configs) {
	}
//	public int partition(Integer key,int numPartitions){
//		int partition = 0;
//		int ikey = key;
//		if(ikey>0){
//			partition = ikey % numPartitions ;
//		}
//		return partition;
//	}

	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		return 1;
	}

	public void close() {
	}

	public int partition(Integer arg0, int arg1) {
		// TODO Auto-generated method stub
		return 0;
	}
}
