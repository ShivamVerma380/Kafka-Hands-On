package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);

        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        String streamURL = "https://stream.wikimedia.org/v2/stream/recentchange";

        Map<String, String> headers = new java.util.HashMap<>();
        headers.put("User-Agent", "kafka-wikimedia-producer/1.0 (shivamvermasv380@gmail.com)"); // Use your email or project name

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(streamURL))
                .headers(Headers.of(headers));

        EventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // we produce for 10 mins and block the program until then
        TimeUnit.MINUTES.sleep(10);

    }
}
