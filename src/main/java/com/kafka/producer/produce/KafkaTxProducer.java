package com.kafka.producer.produce;

import java.util.Properties;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@EnableScheduling
@Component
public class KafkaTxProducer {
	
	@Value("${spring.batchproducer.topicName}")
	private String topic;

	@Value("${spring.batchproducer.bootstrapServer}")
	private String bootstrapServer;

	@Value("${spring.batchproducer.clientId}")
	private String clientId;

	static long delay = 1_000;
	static int number = 0;
	Producer<String, String> producer;

	@Scheduled(fixedRate = 5000)
	void sendMessage() {
		System.out.println("Sending msg to Kafka broker: " + number);
		producer.send(new ProducerRecord<String, String>(topic, "" + number, getEvent()));
		number++;
	}

	/**
	 * This method will return producer instance
	 */
	@PostConstruct
	void createProducer() {
		Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producer = new KafkaProducer<>(producerConfig);
	}

	private static String getEvent() {
		return "This is the Transactional message sending to Kafka Broker";
	}
}
