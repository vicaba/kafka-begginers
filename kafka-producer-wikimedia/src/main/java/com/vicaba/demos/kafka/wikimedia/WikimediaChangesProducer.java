package com.vicaba.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import com.launchdarkly.eventsource.StreamException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class WikimediaChangesProducer {

  public static void main(String[] args) throws InterruptedException {

    final var boostrapServers = "localhost:19092";

    final var properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB batch size
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


    var producer = new KafkaProducer<String, String>(properties);

    final var topic = "wikimedia.recentchanges";

    final var eventHandler = new WikimediaChangeHandler(producer, topic);
    final var url = "https://stream.wikimedia.org/v2/stream/recentchange";
    final var eventSourceBuilder = new EventSource.Builder(URI.create(url));
    final var backgroundEventSourceBuilder = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder);
    try (var backgroundEventSource = backgroundEventSourceBuilder.build()) {
      backgroundEventSource.start();
      TimeUnit.MINUTES.sleep(10);
    }

  }

}
