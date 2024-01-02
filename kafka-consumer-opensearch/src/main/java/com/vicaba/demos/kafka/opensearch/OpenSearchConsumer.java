package com.vicaba.demos.kafka.opensearch;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchConsumer {

  private static final Logger log =
      LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

  private static KafkaConsumer<String, String> createkafkaConsumer() {
    final var groupId = "consumer-opensearch";

    final var properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:19092");
    properties.setProperty("key.deserializer", StringDeserializer.class.getName());
    properties.setProperty("value.deserializer", StringDeserializer.class.getName());
    properties.setProperty("group.id", groupId);
    properties.setProperty("auto.offset.reset", "latest");

    return new KafkaConsumer<String, String>(properties);
  }

  private static RestHighLevelClient createOpenSearchClient() {
    final var connString =
        "https://vhkb900j1s:cfvzmxktxj@mycluster-1605657282.eu-central-1.bonsaisearch.net:443";

    // we build a URI from the connection string
    RestHighLevelClient restHighLevelClient;
    URI connUri = URI.create(connString);
    // extract login information if it exists
    String userInfo = connUri.getUserInfo();

    if (userInfo == null) {
      // REST client without security
      restHighLevelClient = new RestHighLevelClient(
          RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

    } else {
      // REST client with security
      String[] auth = userInfo.split(":");

      CredentialsProvider cp = new BasicCredentialsProvider();
      cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

      restHighLevelClient = new RestHighLevelClient(
          RestClient.builder(
                  new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
              .setHttpClientConfigCallback(
                  httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                      .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
    }

    return restHighLevelClient;
  }

  public static void main(String[] args) throws IOException {
    final var openSearchClient = createOpenSearchClient();
    final var kafkaConsumer = createkafkaConsumer();

    try (openSearchClient; kafkaConsumer) {
      final var indexExists = openSearchClient.indices()
          .exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
      if (!indexExists) {
        final var createIndexRequest = new CreateIndexRequest("wikimedia");
        openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        log.info("The Wikimedia index has been created");
      } else {
        log.info("The Wikimedia index already exists");
      }

      kafkaConsumer.subscribe(List.of("wikimedia.recentchanges"));
      while (true) {
        log.info("Polling...");

        final var records = kafkaConsumer.poll(java.time.Duration.ofMillis(3000));

        for (ConsumerRecord<String, String> record : records) {
          log.info("""
              "Key: {}
              "Value: {}
              "Partition: {}
              "Offset: {}
              """, record.key(), record.value(), record.partition(), record.offset());
          final var indexRequest = new IndexRequest("wikimedia").source(record.value(),
              XContentType.JSON);
          final var response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

          log.info("Inserted 1 document into OpenSearch with id {}", response.getId());
        }

      }

    }
  }
}
