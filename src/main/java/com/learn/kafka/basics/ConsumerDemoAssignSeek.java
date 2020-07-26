package com.learn.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    public static void main(String[] args) {
        String groupId = "my-fourth-application";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER_ADDRESS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assign
        TopicPartition topicPartition = new TopicPartition(Constants.FIRST_TOPIC, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(topicPartition));

        // seek
        consumer.seek(topicPartition, offsetToReadFrom);


        boolean keepReading = true;
        int numberOfMessagesReadSoFar = 0;
        int numberOfMessagesToRead = 5;
        while(keepReading){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            for (ConsumerRecord<String, String> record : records){
                numberOfMessagesReadSoFar++;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                if(numberOfMessagesReadSoFar == numberOfMessagesToRead){
                    keepReading = false;
                    break;
                }
            }
        }
    }
}
