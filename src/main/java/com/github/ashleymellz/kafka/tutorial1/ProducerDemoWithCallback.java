package com.github.ashleymellz.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        // create a logger for my class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "18.222.239.54:9092";

        // create Producer properties
        // Should be referencing https://kafka.apache.org/documentation/

        Properties properties = new Properties();

        // properties.setProperty("bootstrap.servers", bootstrapServers);
        // properties.setProperty("key,serializer", StringSerializer.class.getName());
        // properties.setProperty("value.serializer", StringSerializer.class.getName());

        // however this is considered hardcoding, because we pass specific values but there is a better way
        // the config options from kafka documentation are already imported
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        // Can take in a key, value (string, string)
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // create a producer record
        // keep in mind, topic: and value: are not code-- it's just the IDE identifying the arguments I supplied (key, value) strings
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world!");

        // send data - asynchronous
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time a record is successfully sent or an exception is thrown
                if (e == null){
                    // the record is successfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition:" + recordMetadata.partition() + "\n" +
                            "Offset:" + recordMetadata.offset() + "\n" +
                            "Timestamp:" + recordMetadata.timestamp());
                } else {
                    // we pass in e so we can log the error
                    logger.error("Error while producing", e);
                }
            }
        });

        // to force the data to be produced
        producer.flush();
        // or flush and close
        producer.close();

    }
}
