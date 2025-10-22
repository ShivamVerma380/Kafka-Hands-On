package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        // create producer properties
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");

        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "Shivam Verma!");

        // send data
        producer.send(record);

        producer.send(new ProducerRecord<>("god", "God"));

        producer.flush(); // tell producer to send all data and block until it is done -- synchronous

        // close producer
        producer.close();
    }
}
