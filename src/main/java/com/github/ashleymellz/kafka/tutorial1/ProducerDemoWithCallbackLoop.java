package com.github.ashleymellz.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbackLoop {
    public static void main(String[] args) {
        // create a logger for my class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbackLoop.class);

        String bootstrapServers = "<AWS_HOST>:9092";

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
        // we can override the default partitoner (sticky) and use round robin instead.
        // why is it only being sent to 2/3 of the partitions?
        // but on AWS it produces round robin perfectly?
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        // create the Producer
        // Can take in a key, value (string, string)
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // as a NOTE: sticky partitioning is the NEW Default
        // this means that instead of distributing messages across partitions (round robin)
        // the default partitioner will now use Sticky Partitioning, meaning it will all be sent
        // to one partition, chosen by the Producer
        // https://www.confluent.io/blog/apache-kafka-producer-improvements-sticky-partitioner/
        for(int i=0; i<10; i++){
            // create a producer record
            String topic = "first_topic";
            String value = "hello world" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

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
        }

        // to force the data to be produced
        producer.flush();
        // or flush and close
        producer.close();

    }
}
