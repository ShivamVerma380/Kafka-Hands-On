package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        log.info("I am a Kafka ProducerDemoWithCallback!");

        // create producer properties
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");

        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);



        // send data
        for (int i = 0; i < 10; i++) {
            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "Shivam Verma! " + i);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is sent successfully or an exception is thrown
                    if (e == null) {
                        log.info("Recieved new metadata : \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp() + "\n"
                        );
                    } else {
                        log.error("Error while producing record", e);
                    }
                }
            });

            producer.send(new ProducerRecord<>("god", "God " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is sent successfully or an exception is thrown
                    if (e == null) {
                        log.info("Recieved new metadata : \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp() + "\n"
                        );
                    } else {
                        log.error("Error while producing record", e);
                    }
                }
            });
        }



        producer.flush(); // tell producer to send all data and block until it is done -- synchronous

        // close producer
        producer.close();
    }
}
