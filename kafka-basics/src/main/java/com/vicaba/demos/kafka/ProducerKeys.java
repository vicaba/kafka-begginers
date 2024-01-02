package com.vicaba.demos.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerKeys {

  private static final Logger log =
      LoggerFactory.getLogger(ProducerKeys.class.getSimpleName());

  public static void main(String[] args) {
    log.info("I am a kafka producer with callback!");

    // Create producer properties
    var properties = new Properties();

    // Connect to localhost (kafka)
    properties.setProperty("bootstrap.servers", "localhost:19092");


    // Set Producer properties
    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());
    properties.setProperty("batch.size", "400");


    // Create the producer
    var producer = new KafkaProducer<String, String>(properties);
    final var topic = "demo_java";

    for (int j = 0; j < 2; j++) {

      for (int i = 0; i < 10; i++) {
        final var key = "id_" + i;
        final var value = "hello world" + i;

        // Create a Producer Record
        final var record = new ProducerRecord<>(topic, key, value);

        // Send data
        producer.send(record, (metadata, exception) -> {
          // Executes every time a record is successfully sent or an exception is thrown
          if (exception == null) {
            // The record was successfully sent
            log.info("""
                "Key" {} | Partition: {}
                """, key, metadata.partition());
          } else {
            log.error("Error while producing", exception);
          }
        });
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }


    // Flush and close producer
    producer.flush(); // Explicitly, tell the producer to send all data and block until it's done
    // -- synchronous

    // Flush and close producer
    producer.close(); // tell the producer to send all data and block until it's done -- synchronous
  }
}
