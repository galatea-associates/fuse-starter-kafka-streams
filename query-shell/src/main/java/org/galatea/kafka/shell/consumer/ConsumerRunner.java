package org.galatea.kafka.shell.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.galatea.kafka.shell.consumer.request.ConsumerRequest;
import org.galatea.kafka.shell.domain.ConsumerProperties;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.shell.stores.ConsumerRecordTable;
import org.galatea.kafka.starter.util.Translator;
import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerRunner implements Runnable, ApplicationContextAware {

  @Getter
  private final ConsumerProperties properties = new ConsumerProperties();
  private final Consumer<GenericRecord, GenericRecord> consumer;
  private static final Duration POLL_MAX_DURATION = Duration.ofSeconds(1);
  private final Translator<ConsumerRecord<GenericRecord, GenericRecord>, KeyValue<DbRecordKey, DbRecord>> localRecordTranslator;
  private ConfigurableApplicationContext applicationContext;


  @Override
  public void run() {
    log.info("Started Thread");

    // TODO: do deserialization on a topic basis, to allow for dynamic definition of serdes to use
    // TODO: also would allow the consumer to reject a topic due to invalid configuration without crashing
    try {
      while (true) {
        if (properties.isAssignmentUpdated()) {
          log.info("Updating consumer assignment {}", properties.getAssignment());
          updateConsumerAssignment(consumer, properties);
          properties.setAssignmentUpdated(false);
        }
        if (properties.getAssignment().isEmpty()) {
          trySleep(1000);
          continue;
        }

        consumer.poll(POLL_MAX_DURATION).forEach(record -> {
          KeyValue<DbRecordKey, DbRecord> localRecord = localRecordTranslator.apply(record);
          subscribedStores(record.topic()).forEach(store -> store.addRecord(localRecord));
          updateStatistics(record);
        });

        if (!properties.getPendingRequests().isEmpty()) {
          List<ConsumerRequest<?>> requests = new ArrayList<>();
          properties.getPendingRequests().drainTo(requests);
          requests.forEach(request -> request.internalFulfillRequest(consumer));
        }

      }
    } catch (Exception e) {
      System.err.println("Kafka consumer has crashed. Shell will be closed.");
      e.printStackTrace();
      System.exit(SpringApplication.exit(applicationContext, () -> 0));
    }
  }

  private void updateStatistics(ConsumerRecord<GenericRecord, GenericRecord> record) {
    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
    properties.getConsumedMessages()
        .compute(topicPartition, (key, aLong) -> aLong != null ? aLong + 1 : 1);
    properties.getLatestOffset().put(topicPartition, record.offset());
  }

  private Set<ConsumerRecordTable> subscribedStores(String topic) {
    return properties.getStoreSubscription().computeIfAbsent(topic, s -> new HashSet<>());
  }

  private static void updateConsumerAssignment(Consumer<GenericRecord, GenericRecord> consumer,
      ConsumerProperties properties) {

    consumer.assign(properties.getAssignment());
    log.info("Updated consumer assignment: {}", properties);

    consumer.seekToBeginning(properties.getSeekBeginningAssignment());
    // reset number of consumed messages for each partition that has been reset to 0
    properties.getSeekBeginningAssignment()
        .forEach(topicPartition -> properties.getConsumedMessages().put(topicPartition, 0L));
    log.info("Seek to beginning for {}", properties.getSeekBeginningAssignment());

    properties.getSeekBeginningAssignment().clear();
  }

  private static void trySleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      log.warn("Sleep interrupted: ", e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

    this.applicationContext = (ConfigurableApplicationContext) applicationContext;
  }
}
