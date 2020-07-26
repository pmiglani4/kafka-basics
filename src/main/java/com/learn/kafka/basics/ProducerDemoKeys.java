package com.learn.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public  static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER_ADDRESS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer producer = new KafkaProducer<String, String>(properties);

        // create a producer records
        String topic = "first_topic";
        for(int i=1;i<=10;i++) {
            String key = "id_" + i;
            String value = "hello world " + i;
            final ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

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
            }).get(); // block the .send() to make it synchronous - don't do this in production!
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();


    }
}
