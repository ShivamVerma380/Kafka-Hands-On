package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) {
        log.info("I am a Kafka ProducerDemoWithKeys !");

        // create producer properties
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");

        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        for (int j = 0; j < 2; j++) {
            // send data
            for (int i = 0; i < 10; i++) {
                // create producer record
                String key = "key-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>("god", key, "God " + i);

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is sent successfully or an exception is thrown
                        if (e == null) {
                            log.info("Key : " + key + " | " + "Partition : " + recordMetadata.partition() + "\n");
                        } else {
                            log.error("Error while producing record", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }





        producer.flush(); // tell producer to send all data and block until it is done -- synchronous

        // close producer
        producer.close();
    }
}
