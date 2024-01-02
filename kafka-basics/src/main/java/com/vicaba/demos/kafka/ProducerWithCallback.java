package com.vicaba.demos.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallback {

  private static final Logger log =
      LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());

  public static void main(String[] args) {
    log.info("I am a kafka producer with callback!");

    // Create producer properties
    var properties = new Properties();

    // Connect to localhost (kafka)
    properties.setProperty("bootstrap.servers", "localhost:19092");


    // Set Producer properties
    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());

    // Create the producer
    var producer = new KafkaProducer<String, String>(properties);

    // Create a Producer Record
    var record = new ProducerRecord<String, String>("demo_java", "hello world");

    // Send data
    producer.send(record, (metadata, exception) -> {
      // Executes every time a record is successfully sent or an exception is thrown
      if (exception == null) {
        // The record was successfully sent
        log.info("""
            Received new metadata.
            "Topic: {}
            "Partition: {}
            "Offset: {}
            "Timestamp: {}
            """, metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
      } else {
        log.error("Error while producing", exception);
      }
    });

    // Flush and close producer
    producer.flush(); // Explicitly, tell the producer to send all data and block until it's done
    // -- synchronous

    // Flush and close producer
    producer.close(); // tell the producer to send all data and block until it's done -- synchronous
  }
}
