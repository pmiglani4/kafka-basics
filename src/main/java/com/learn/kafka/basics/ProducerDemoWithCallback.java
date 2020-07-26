package com.learn.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public  static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
    public static void main(String[] args) {

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER_ADDRESS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer producer = new KafkaProducer<String, String>(properties);

        // create a producer records
        for (int i=0; i<10; i++ ) {
            // create a producer record
            final ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "hello world " + Integer.toString(i));

            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        logger.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Value:" + record.value() +"\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    }else{
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();


    }
}
