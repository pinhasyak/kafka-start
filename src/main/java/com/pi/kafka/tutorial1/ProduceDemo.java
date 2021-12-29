package com.pi.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProduceDemo {

    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String TOPIC_NAME = "first_topic";

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProduceDemo.class);
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i<10; i++) {
            String value = "hello world" + i;
            String key = "_id" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata. \n Topic: {} \n Partition: {} \n Offset: {} \n Timestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        logger.info("Error while producing", e);
                    }
                }
            });
        }
        producer.flush();

        producer.close();
    }
}
