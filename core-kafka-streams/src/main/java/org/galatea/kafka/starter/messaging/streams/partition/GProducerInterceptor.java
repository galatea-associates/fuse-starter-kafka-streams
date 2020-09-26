package org.galatea.kafka.starter.messaging.streams.partition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.galatea.kafka.starter.messaging.KafkaStreamsAutoconfig;
import org.galatea.kafka.starter.messaging.streams.domain.ConfiguredHeaders;

@Slf4j
public class GProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

  @Setter
  private static AdminClient kafkaAdminClient;
  @Getter(AccessLevel.PACKAGE)
  private static final Map<String, Integer> topicPartitions = new HashMap<>();

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
    Iterator<Header> headers = producerRecord.headers()
        .headers(ConfiguredHeaders.PARTITION_KEY.getKey()).iterator();
    Integer assignPartition = producerRecord.partition();
    if (headers.hasNext() && assignPartition == null) {
      int partitions = numberPartitions(producerRecord.topic());
      Header partKeyHeader = headers.next();
      String hashString = new String(partKeyHeader.value());
      int hashCode = hashString.hashCode();
      assignPartition = hashCode % partitions;
      log.info("Assigning record to partition {} based on hashing string {}: {}", assignPartition,
          hashString, producerRecord);
    }
    return new ProducerRecord<>(producerRecord.topic(), assignPartition, producerRecord.timestamp(),
        producerRecord.key(), producerRecord.value(), producerRecord.headers());
  }

  private int numberPartitions(String topic) {
    if (topicPartitions.containsKey(topic)) {
      return topicPartitions.get(topic);
    }
    Exception thrown;
    try {
      Objects.requireNonNull(kafkaAdminClient,
          String.format("%s requires import of %s", getClass(), KafkaStreamsAutoconfig.class));
      int size = kafkaAdminClient.describeTopics(Collections.singleton(topic)).all().get()
          .get(topic).partitions().size();
      topicPartitions.put(topic, size);
      return size;
    } catch (InterruptedException e) {
      log.error("Interrupted while fetching topic configuration", e);
      thrown = e;
    } catch (ExecutionException e) {
      log.error("Error fetching topic configuration {}", topic, e);
      thrown = e;
    }
    throw new RuntimeException(
        String.format("Could not determine number of partitions for topic %s", topic), thrown);
  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    // do nothing
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public void configure(Map<String, ?> map) {
    // do nothing
  }
}
