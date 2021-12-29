package com.pi.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreds {
    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String GROUP_ID = "my-demo-threads-application";
    public static final String TOPIC_NAME = "first_topic";

    public static void main(String[] args) {
        new ConsumerDemoWithThreds().run();
    }

    public ConsumerDemoWithThreds() {
    }

    public void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreds.class);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        logger.info("Creating the consumer thread");
        Runnable consumerRunnable = new ConsumerRunnable(countDownLatch);
        // start the thread
        Thread consumerTread = new Thread(consumerRunnable);
        consumerTread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook( new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable)consumerRunnable).shutdown();
            try {
                countDownLatch.await();
            }catch (InterruptedException interruptedException){
                logger.error("Application got interrupted", interruptedException);
            }
            logger.info("Application has exited");
        }));


        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private final CountDownLatch countDownLatch;
        private final KafkaConsumer<String, String> consumer;
        public ConsumerRunnable(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer =  new KafkaConsumer<>(properties);
            this.consumer.subscribe(Collections.singleton(TOPIC_NAME));
        }

        @Override
        public void run() {
            try {
                while (true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord record : records){
                        logger.info("Key: {}, Value: {}, \n Partition: {}, Offset: {}", record.key(), record.value(), record.partition(), record.offset());
                    }
                }
            }catch (WakeupException wakeupException){
                logger.info("Received shutdown signal!");
            }finally {
                consumer.close();
                countDownLatch.countDown();
            }
        }

        public void shutdown(){
            this.consumer.wakeup();
        }

    }
}
