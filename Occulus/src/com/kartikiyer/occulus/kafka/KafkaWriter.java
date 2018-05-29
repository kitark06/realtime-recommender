package com.kartikiyer.occulus.kafka;


import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;


public class KafkaWriter<K, V> implements AutoCloseable
{
	Logger				log	= Logger.getLogger(KafkaWriter.class);

	private Properties		kafkaProps;
	private String			topicName;
	private Producer<K, V>	producer;


	public KafkaWriter(String kafkaClusterIp, String clientId, String keySerializer, String valueSerializer, String topicName)
	{
		kafkaProps = new Properties();
		kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClusterIp);
		kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
		kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
		kafkaProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		this.topicName = topicName;
	}

	public KafkaWriter(Properties kafkaProps , String topicName)
	{
		this.kafkaProps = kafkaProps;
		this.topicName = topicName;
	}

	public void writeMessage(K key, V value)
	{
		producer = new KafkaProducer<>(kafkaProps);
		ProducerRecord<K, V> record = new ProducerRecord<>(topicName, key, value);
		Future<RecordMetadata> metadata = producer.send(record, (metadata1, exception) ->
		{
			if (exception != null)
				log.error(exception.toString());
		});
	}

	@Override
	public void close() throws Exception
	{
		producer.close();
	}
}
