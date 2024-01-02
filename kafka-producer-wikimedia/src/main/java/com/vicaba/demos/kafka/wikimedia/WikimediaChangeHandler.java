package com.vicaba.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WikimediaChangeHandler implements BackgroundEventHandler {

  private KafkaProducer<String, String> kafkaProducer;

  private String topic;

  private final Logger log =
      LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

  public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
    this.kafkaProducer = kafkaProducer;
    this.topic = topic;
  }

  @Override
  public void onOpen() {
    // nothing here
  }

  @Override
  public void onClosed(){
    kafkaProducer.close();
  }

  @Override
  public void onMessage(String s, MessageEvent messageEvent) {
    log.info("Received message: {}", messageEvent.getData());
    // asynchronous
    kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
  }

  @Override
  public void onComment(String s) {
    // nothing here
  }

  @Override
  public void onError(Throwable throwable) {
    log.error("Error while consuming", throwable);
  }
}
