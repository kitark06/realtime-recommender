package com.kartikiyer.occulus.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

public class KafkaReader<K ,V >
{
	Logger				log	= Logger.getLogger(this.getClass());

	private Properties		kafkaProps;
	private String			topicName;
	
	public KafkaReader(String kafkaClusterIp, String clientId, String keyDESerializer, String valueDESerializer, String consumerGroupId , String topicName)
	{
		kafkaProps = new Properties();
		
		kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClusterIp);
		kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDESerializer);
		kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDESerializer);
		kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		
		this.topicName = topicName;
	}

	public KafkaReader(Properties kafkaProps , String topicName)
	{
		this.kafkaProps = kafkaProps;
		this.topicName = topicName;
	}
	
	public void readMessage()
	{
		
		// Done to get more fine grained control over the commit freq ..
		// commits will be handled manually depending on app logic to prevent missing/dup msg processing in case of failure
		kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		
		
		// for even distro of partitions in spite of having a lobsided topic-partition scenario 
		kafkaProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");
		
		
		

	}
}
