package com.vicaba.demos.kafka;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

  private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

  public static void main(String[] args) {
    log.info("I am a kafka consumer!");

    final var groupId = "my-java-application";
    final var topic = "demo_java";

    final var properties = new Properties();

    properties.setProperty("bootstrap.servers", "localhost:19092");

    properties.setProperty("key.deserializer", StringDeserializer.class.getName());
    properties.setProperty("value.deserializer", StringDeserializer.class.getName());

    properties.setProperty("group.id", groupId);
    properties.setProperty("auto.offset.reset", "earliest");

    final var consumer = new KafkaConsumer<String, String>(properties);

    final var mainThread = Thread.currentThread();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
      consumer.wakeup();
      // join the main thread to allow the execution of the code in the main thread
      try {
        mainThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }));

    try {
      consumer.subscribe(List.of(topic));

      while (true) {
        log.info("Polling...");

        final var records = consumer.poll(java.time.Duration.ofMillis(1000));

        records.forEach(record -> {
          log.info("""
              "Key: {}
              "Value: {}
              "Partition: {}
              "Offset: {}
              """, record.key(), record.value(), record.partition(), record.offset());
        });
      }
    } catch (WakeupException e) {
      log.info("Received shutdown signal");
    } catch (Exception e) {
      log.error("Error", e);
    } finally {
      consumer.close();
      log.info("Consumer closed");
    }

  }
}
