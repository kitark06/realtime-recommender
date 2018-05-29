package com.kartikiyer.occulus;


import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.kartikiyer.occulus.kafka.KafkaWriter;
import com.kartikiyer.occulus.kafka.RandomInputGenerator;


public class Visualizer
{
	private void doAwesomeStuffs() throws Exception
	{
		KafkaWriter<Integer, String> writer = new KafkaWriter<>("quickstart.cloudera:9092", "OcculusProducerOne", IntegerSerializer.class.getName(), StringSerializer.class.getName(), "Optic");

		RandomInputGenerator rng = new RandomInputGenerator();

		/*new Thread(() ->
		{
			while (true)
			{
				writer.writeMessage(1, rng.getRandomInput());
				try
				{
					Thread.sleep(200);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}).start();*/

		Properties kafkaProps = new Properties();
		kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092");

		FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>("Optic", new SimpleStringSchema(), kafkaProps);
		kafkaConsumer.setStartFromEarliest();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> stream = env.addSource(kafkaConsumer);

		stream.map(x -> x.toUpperCase()).print();
		env.execute();
	}

	public static void main(String[] args) throws Exception
	{
		new Visualizer().doAwesomeStuffs();
	}
}
