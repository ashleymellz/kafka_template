package com.github.ashleymellz.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // create a logger for my class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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

        for(int i=0; i<10; i++){
            // create a producer record

            String topic = "first_topic";
            String value = "hello world " + Integer.toString(i);
            // so now in a callback, what we should be seeing is that the same keys
            // go to the same partitions
            String key = "Key_" + Integer.toString(i);

            // keep in mind, topic: and value: are not code-- it's just the IDE identifying the arguments I supplied (key, value) strings
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key:" + key); //log the key
            // when we run the code we see Key_0 goes to partition 0
            // key_1 partition 0
            // key_2 partition 0
            // key_3 partiiton 0
            // key_4 partition 0
            // key_5 partition 0

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
            }).get(); //block the .send() to make it synchronous (DO NOT DO IN PRODUCTION)
        }

        // to force the data to be produced
        // producer.flush();
        // or flush and close
        producer.close();

    }
}
