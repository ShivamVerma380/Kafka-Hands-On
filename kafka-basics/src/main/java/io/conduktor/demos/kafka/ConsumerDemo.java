package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        String groupId = "consumer-my-group-id-1";
        String topic = "god";

        // create producer properties
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");


        // Create consumer configs
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        props.setProperty("group.id", groupId);

        props.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(List.of(topic));

        while (true) {
            log.info("Polling for new data...");

            ConsumerRecords<String , String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord record : records) {
                log.info("Key : " + record.key() + " | " + "Value : " + record.value() + "\n");
            }
        }

    }
}
