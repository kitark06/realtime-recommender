package com.kartikiyer.occulus;


import java.util.Properties;

import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;


public class KafkaConsumerTX
{
	public static void main(String[] args) throws Exception
	{
		Logger log = Logger.getLogger(KafkaConsumerTX.class);
		
		String jarFiles = "C:/Kartik/Workspace/xWING/FlinkTransformer/target/FlinkTransformer-Mark1.jar";
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123 , jarFiles);
		
		Properties kafkaProps = new Properties();
		kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092");

		FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>("topicMark", new SimpleStringSchema(), kafkaProps);
		kafkaConsumer.setStartFromEarliest();

		DataStream<String> stream = env.addSource(kafkaConsumer);

		log.trace("log msg");
		stream.map ( x -> x.toUpperCase() ) .writeAsText("C:/Kartik/prime.txt");
		env.execute();
	}
}
