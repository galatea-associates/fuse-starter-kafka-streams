package org.galatea.kafka.shell.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.galatea.kafka.shell.consumer.request.ConsumerRequest;
import org.galatea.kafka.shell.controller.KafkaSerdeController;
import org.galatea.kafka.shell.controller.RecordStoreController;
import org.galatea.kafka.shell.domain.ConsumerProperties;
import org.galatea.kafka.shell.domain.DbRecord;
import org.galatea.kafka.shell.domain.DbRecordKey;
import org.galatea.kafka.shell.stores.ConsumerRecordTable;
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
  private final Consumer<byte[], byte[]> consumer;
  private final KafkaSerdeController kafkaSerdeController;
  private final RecordStoreController recordStoreController;
  private static final Duration POLL_MAX_DURATION = Duration.ofSeconds(1);
  private ConfigurableApplicationContext applicationContext;
  private Set<String> errorTopics = new HashSet<>();
  private final AtomicLong messagesConsumed = new AtomicLong(0);

  @Override
  public void run() {
    log.info("Started Thread");

    try {
      while (true) {
        properties.getHistoricalStatistic().reportConsumedMessageCount(messagesConsumed.get());

        if (!errorTopics.isEmpty()) {
          removeErrorTopicsFromAssignment(properties, errorTopics);
          errorTopics.forEach(errorTopic -> properties.getStoreSubscription().get(errorTopic)
              .forEach(table -> recordStoreController.deleteTable(table.getName())));
          errorTopics
              .forEach(errorTopic -> properties.getStoreSubscription().get(errorTopic).clear());
          errorTopics.clear();
        }

        if (properties.isAssignmentUpdated()) {
          log.info("Updating consumer assignment {}", properties.getAssignment());
          updateConsumerAssignment(consumer, properties);
          properties.setAssignmentUpdated(false);
        }

        if (!properties.getPendingRequests().isEmpty()) {
          List<ConsumerRequest<?>> requests = new ArrayList<>();
          properties.getPendingRequests().drainTo(requests);
          requests.forEach(request -> request.internalFulfillRequest(consumer));
        }

        if (consumer.assignment().isEmpty()) {
          trySleep(POLL_MAX_DURATION.toMillis());
          continue;
        }

        consumer.poll(POLL_MAX_DURATION).forEach(record -> {
          messagesConsumed.incrementAndGet();
          if (errorTopics.contains(record.topic())) {
            return;
          }
          try {
            ConsumerRecord<DbRecordKey, DbRecord> localRecord = kafkaSerdeController
                .deserialize(record);
            subscribedStores(record.topic()).forEach(store -> store.addRecord(localRecord));
            updateStatistics(localRecord);
          } catch (Exception e) {
            errorTopics.add(record.topic());
            System.err.println(String.format("Topic %s cannot be consumed. Subscribed stores will "
                + "be deleted. Run 'status' for error details", record.topic()));
            properties.getTopicExceptions().put(record.topic(), e);
          }
        });
      }
    } catch (Exception e) {
      System.err.println("Kafka consumer has crashed. Shell will be closed.");
      e.printStackTrace();
      System.exit(SpringApplication.exit(applicationContext, () -> 0));
    }
  }

  private void removeErrorTopicsFromAssignment(ConsumerProperties properties,
      Set<String> errorTopics) {

    // TODO: trying to subscribe to same topic again with different configuration (after error) doesn't do anything
    List<TopicPartition> assignmentWithoutErrorTopics = properties.getAssignment()
        .stream()
        .filter(topicPartition -> !errorTopics.contains(topicPartition.topic()))
        .collect(Collectors.toList());
    properties.getAssignment().clear();
    properties.getAssignment().addAll(assignmentWithoutErrorTopics);
    properties.setAssignmentUpdated(true);
  }

  private void updateStatistics(ConsumerRecord<DbRecordKey, DbRecord> record) {
    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
    properties.getConsumedMessages()
        .compute(topicPartition, (key, aLong) -> aLong != null ? aLong + 1 : 1);
    properties.getLatestOffset().put(topicPartition, record.offset());
  }

  private Set<ConsumerRecordTable> subscribedStores(String topic) {
    return properties.getStoreSubscription().computeIfAbsent(topic, s -> new HashSet<>());
  }

  private static void updateConsumerAssignment(Consumer<byte[], byte[]> consumer,
      ConsumerProperties properties) {

    consumer.assign(properties.getAssignment());
    log.info("Updated consumer assignment: {}", properties);

    if (!properties.getAssignment().isEmpty()) {
      consumer.seekToBeginning(properties.getSeekBeginningAssignment());
      // reset number of consumed messages for each partition that has been reset to 0
      properties.getSeekBeginningAssignment()
          .forEach(topicPartition -> properties.getConsumedMessages().put(topicPartition, 0L));
      log.info("Seek to beginning for {}", properties.getSeekBeginningAssignment());

      properties.getSeekBeginningAssignment().clear();
    }
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
