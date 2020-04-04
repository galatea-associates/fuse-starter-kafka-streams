package org.galatea.kafka.shell.controller;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.galatea.kafka.shell.consumer.ConsumerRunner;
import org.galatea.kafka.shell.consumer.request.ConsumerRequest;
import org.galatea.kafka.shell.consumer.request.ConsumerTopicOffsetsRequest;
import org.galatea.kafka.shell.consumer.request.TopicOffsetsRequest;
import org.galatea.kafka.shell.domain.ConsumerProperties;
import org.galatea.kafka.shell.domain.OffsetMap;
import org.galatea.kafka.shell.stores.ConsumerRecordTable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerThreadController {

  private final ConsumerRunner runner;
  private final AdminClient adminClient;

  public Map<TopicPartition, OffsetMap> getConsumerOffsets()
      throws InterruptedException {

    if (runner.getProperties().getAssignment().isEmpty()) {
      return Collections.emptyMap();
    }
    return getRequestResponse(new ConsumerTopicOffsetsRequest());
  }

  public ConsumerProperties consumerProperties() {
    return runner.getProperties();
  }

  public ConsumerThreadController(ConsumerRunner runner, AdminClient adminClient) {
    this.runner = runner;
    this.adminClient = adminClient;
    Thread consumerThread = new Thread(null, runner, "KafkaConsumer");
    consumerThread.start();
  }

  public boolean addTopicToAssignment(String topic) {
    log.info("Adding topic {} to assignment", topic);
    try {
      List<TopicPartition> addToAssignment = getTopicPartition(topic);
      Set<TopicPartition> currentAssignment = runner.getProperties().getAssignment();

      // remove new topic from existing assignment, so it will be added in with beginning offsets
      // since this will cause subscribed stores to potentially receive older records than their
      // latest, each store will need to track the latest offset for each partition and discard
      // records with earlier offsets
      currentAssignment = currentAssignment.stream()
          .filter(topicPartition -> !topicPartition.topic().equals(topic))
          .collect(Collectors.toSet());
      currentAssignment.addAll(addToAssignment);

      runner.getProperties().getSeekBeginningAssignment().addAll(addToAssignment);
      runner.getProperties().setAssignment(currentAssignment);
      runner.getProperties().setAssignmentUpdated(true);
    } catch (Exception e) {
      log.warn("Could not get Topic details:", e);
      return false;
    }
    return true;
  }

  private List<TopicPartition> getTopicPartition(String topic)
      throws Exception {

    TopicDescription retrievedDescription = adminClient.describeTopics(Collections.singleton(topic))
        .all().get().get(topic);

    return retrievedDescription.partitions().stream()
        .map(topicPartInfo -> new TopicPartition(topic, topicPartInfo.partition()))
        .collect(Collectors.toList());
  }

  private Set<ConsumerRecordTable> subscribedStores(String topic) {
    return runner.getProperties().getStoreSubscription()
        .computeIfAbsent(topic, s -> new HashSet<>());
  }

  public void addStoreAssignment(String topic, ConsumerRecordTable store) {
    subscribedStores(topic).add(store);
  }

  public Set<ConsumerRecordTable> removeTopicAssignment(String topicName) {
    Set<TopicPartition> assignment = runner.getProperties().getAssignment();
    List<TopicPartition> newAssignment = assignment.stream()
        .filter(topicPart -> !topicPart.topic().equals(topicName)).collect(Collectors.toList());
    assignment.clear();
    assignment.addAll(newAssignment);
    runner.getProperties().setAssignmentUpdated(true);

    Set<ConsumerRecordTable> subscribed = runner.getProperties().getStoreSubscription()
        .get(topicName);
    Set<ConsumerRecordTable> outputSet = new HashSet<>(subscribed);
    subscribed.clear();
    runner.getProperties().getStoreSubscription().remove(topicName);
    return outputSet;
  }

  private <T> T getRequestResponse(ConsumerRequest<T> request) throws InterruptedException {
    runner.getProperties().getPendingRequests().add(request);
    return request.get();
  }

  public Map<TopicPartition, OffsetMap> getTopicOffsets(String topic)
      throws InterruptedException {
    return getRequestResponse(new TopicOffsetsRequest(Collections.singletonList(topic)));
  }

  public Map<TopicPartition, OffsetMap> getTopicOffsets(Collection<String> topics)
      throws InterruptedException {
    return getRequestResponse(new TopicOffsetsRequest(topics));
  }

}
