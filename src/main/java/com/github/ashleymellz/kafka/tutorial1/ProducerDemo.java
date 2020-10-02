package com.github.ashleymellz.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
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
        producer.send(record);
        // to force the data to be produced
        // producer.flush();
        // or flush and close
        producer.close();

    }
}
